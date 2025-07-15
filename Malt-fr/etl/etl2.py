"""
Malt Data Processor (ETL): Raw to Structured CSV

PURPOSE:
  (Same as previous version)

VERSION: 1.1.0 (Added 'include_ids' for targeted processing)

USAGE:
  - This script can be imported and run from a Jupyter Notebook or another Python script.
  - Call the main function `process_malt_data()`.
  - An optional `limit` argument can be provided to process a subset of profiles.
  - An optional `include_ids` list can be provided to process only specific profiles.

Author: @AyoubFrihaoui
Version: 1.1.0
"""

import os
import re
import json
import logging
import time
import pandas as pd
from lxml import html
from tqdm import tqdm
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timezone

# --- Configuration ---
class Config:
    RAW_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\raw_data"
    HTML_ROOT = r"H:\Data Extraction FSP\Malt-fr\raw_data\malt_fr\developpeur\en_télétravail\profiles\developpeur\en_télétravail\profiles"
    PROCESSED_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\processed_data"
    TABLE_NAMES = [
        "profiles", "profile_skills", "profile_badges", "profile_languages",
        "profile_categories", "portfolio_items", "reviews", "recommendations",
        "experiences", "education", "certifications"
    ]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    """Safely retrieve a nested value from a dictionary."""
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data

def find_and_load_json_files(root_path: str) -> List[str]:
    """Recursively finds all 'page_*.json' files."""
    json_files = []
    logger.info(f"Searching for raw JSON files in: {root_path}")
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            if filename.startswith("page_") and filename.endswith(".json"):
                json_files.append(os.path.join(dirpath, filename))
    logger.info(f"Found {len(json_files)} raw JSON files to process.")
    return json_files

def extract_profiles_from_json(json_files: List[str]) -> Dict[str, Dict]:
    """Extracts unique profiles from a list of JSON files."""
    profiles = {}
    logger.info("Extracting unique profiles from JSON files...")
    for file_path in tqdm(json_files, desc="Reading JSON files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for profile_data in data.get("profiles", []):
                profile_id = profile_data.get("id")
                if profile_id and profile_id not in profiles:
                    profiles[profile_id] = profile_data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read or parse {file_path}: {e}")
    logger.info(f"Extracted {len(profiles)} unique profiles.")
    return profiles

# --- HTML Parsing Functions ---
def parse_ld_json(html_content: str) -> Optional[Dict]:
    """Parses HTML to find and load the second 'application/ld+json' script."""
    ld_json_scripts = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
    if len(ld_json_scripts) > 1:
        try:
            return json.loads(ld_json_scripts[1])
        except json.JSONDecodeError:
            return None
    return None

def extract_with_xpath(tree: html.HtmlElement, xpath: str) -> Optional[str]:
    """Extracts text content from the first element matching the XPath."""
    elements = tree.xpath(xpath)
    if elements:
        return elements[0].text_content().strip()
    return None

def extract_list_with_xpath(tree: html.HtmlElement, xpath_template: str) -> List[str]:
    """Extracts a list of text content by iterating an indexed XPath."""
    items = []
    index = 1
    while True:
        xpath = xpath_template.format(index=index)
        element = extract_with_xpath(tree, xpath)
        if element:
            items.append(element)
            index += 1
        else:
            break
    return items

def extract_nested_list_with_xpath(tree: html.HtmlElement, xpath_template: str) -> List[str]:
    """Extracts a list of text from a nested structure."""
    items = []
    outer_index = 1
    while True:
        inner_index = 1; found_in_outer = False
        while True:
            xpath = xpath_template.format(index_cat=outer_index, index_item=inner_index)
            element = extract_with_xpath(tree, xpath)
            if element:
                items.append(element); inner_index += 1; found_in_outer = True
            else: break
        if not found_in_outer: break
        outer_index += 1
    return items

# --- Main Processor Class ---
class MaltDataProcessor:
    """Orchestrates the ETL process from raw files to structured CSVs."""
    def __init__(self, output_run_path: str):
        self.data_frames: Dict[str, List[Dict]] = {name: [] for name in Config.TABLE_NAMES}
        self.retrieved_at = datetime.now(timezone.utc).isoformat()
        self.output_path = output_run_path
        self.processed_count = 0
        self.error_count = 0

    def process_profile(self, profile_id: str, json_data: Dict):
        """Processes a single profile, combining data from JSON and its corresponding HTML file."""
        html_file_path = os.path.join(Config.HTML_ROOT, f"{profile_id}.html")
        html_content = None; ld_json_data = {}; tree = None
        if os.path.exists(html_file_path):
            try:
                with open(html_file_path, 'r', encoding='utf-8') as f: html_content = f.read()
                tree = html.fromstring(html_content)
                parsed_ld = parse_ld_json(html_content)
                if parsed_ld: ld_json_data = parsed_ld
            except (IOError, html.etree.ParserError) as e:
                logger.warning(f"Could not read or parse HTML for profile {profile_id}: {e}")
                self.error_count += 1; return
        else:
            logger.debug(f"HTML file not found for profile {profile_id}. Proceeding with JSON data only.")

        # --- 1. PROFILES TABLE ---
        full_name = safe_get(ld_json_data, ['name'], ''); first_name = json_data.get('firstName', '')
        last_name = full_name.replace(first_name, '').strip() if full_name and first_name else None
        profile_row = {
            "profile_id": profile_id, "first_name": first_name, "last_name": last_name,
            "headline": json_data.get('headline'), "city": safe_get(json_data, ['location', 'city']),
            "location_type": safe_get(json_data, ['location', 'locationType']),
            "price_amount": safe_get(json_data, ['price', 'value', 'amount']),
            "price_currency": safe_get(json_data, ['price', 'value', 'currency']),
            "profile_url": safe_get(ld_json_data, ['url']), "photo_url": safe_get(ld_json_data, ['image']),
            "availability_status": safe_get(json_data, ['availability', 'status']),
            "work_availability": safe_get(json_data, ['availability', 'workAvailability']),
            "next_availability_date": safe_get(json_data, ['availability', 'nextAvailabilityDate']),
            "stats_missions_count": safe_get(json_data, ['stats', 'missionsCount']),
            "stats_recommendations_count": safe_get(json_data, ['stats', 'recommendationsCount']),
            "aggregate_rating_value": float(re.search(r'"ratingValue"\s*:\s*([\d.]+)', html_content).group(1)) if re.search(r'"ratingValue"\s*:\s*([\d.]+)', html_content) else None,
            "aggregate_rating_count": int(re.search(r'"ratingCount"\s*:\s*(\d+)', html_content).group(1)) if re.search(r'"ratingCount"\s*:\s*(\d+)', html_content) else None,
            "aggregate_review_count": int(re.search(r'"reviewCount"\s*:\s*(\d+)', html_content).group(1)) if re.search(r'"reviewCount"\s*:\s*(\d+)', html_content) else None,
            "aggregate_best_rating": int(re.search(r'"bestRating"\s*:\s*(\d+)', html_content).group(1)) if re.search(r'"bestRating"\s*:\s*(\d+)', html_content) else None,
            #"aggregate_worst_rating": safe_get(ld_json_data, ['aggregateRating', 'worstRating']),
            "aggregate_worst_rating": int(re.search(r'"worstRating"\s*:\s*(\d+)', html_content).group(1)) if re.search(r'"worstRating"\s*:\s*(\d+)', html_content) else None,
            "retrieved_at": self.retrieved_at,
        }
        if tree is not None:
            profile_row.update({
                "experience_years_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[2]/span[2]'),
                "response_rate_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[3]/span[2]'),
                "response_time_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[4]/span[2]'),
                "profile_description": extract_with_xpath(tree, '/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[3]/div/div/div[2]'),
                "has_signed_charter": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[1]/div/div[2]/p')),
                "has_verified_email": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[4]/div/div[2]')),
            })
        self.data_frames["profiles"].append(profile_row)

        # --- One-to-Many Tables ---
        for skill in json_data.get('skills', []): self.data_frames["profile_skills"].append({"profile_id": profile_id, "skill_name": skill.get('label'), "is_certified": skill.get('certified')})
        for badge in json_data.get('badges', []): self.data_frames["profile_badges"].append({"profile_id": profile_id, "badge_name": badge})
        for item in json_data.get('portfolio', []): self.data_frames["portfolio_items"].append({"profile_id": profile_id, "item_index": item.get('index'), "title": item.get('title'), "item_type": item.get('type'), "low_res_url": safe_get(item, ['picture', 'lowResolutionUrl']), "med_res_url": safe_get(item, ['picture', 'mediumResolutionUrl']), "high_res_url": safe_get(item, ['picture', 'highResolutionUrl'])})
        for i, review in enumerate(safe_get(ld_json_data, ['review'], [])): self.data_frames["reviews"].append({"profile_id": profile_id, "review_index": i, "author_name": safe_get(review, ['author', 'name']), "author_image_url": safe_get(review, ['author', 'image']), "date_published": review.get('datePublished'), "review_body": review.get('reviewBody')})
        
        if tree is not None:
            for lang in extract_list_with_xpath(tree, '//*[@id="languages-section"]/div/ul/li[{index}]/p[1]'): self.data_frames["profile_languages"].append({"profile_id": profile_id, "language_name": lang})
            for cat in extract_nested_list_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[4]/div/div[{index_cat}]/ul/li[{index_item}]/a/span'): self.data_frames["profile_categories"].append({"profile_id": profile_id, "category_name": cat})
            
            index = 1;
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[7]/div/div/div[{index}]'
                if not tree.xpath(base_xpath): break
                name = extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/span[2]') or extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/a/span/span[2]')
                self.data_frames["recommendations"].append({"profile_id": profile_id, "recommendation_index": index - 1, "recommender_name": name, "recommender_company": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[2]'), "recommendation_date": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[2]'), "recommendation_desc": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[3]')}); index += 1
            
            index = 1;
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[5]/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["experiences"].append({"profile_id": profile_id, "experience_index": index - 1, "company_name": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[1]/div'), "title": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[2]'), "date_range": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[1]/span'), "location": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[2]'), "description": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[4]/div')}); index += 1

            index = 1;
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[8]/div/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["education"].append({"profile_id": profile_id, "education_index": index - 1, "degree": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[1]/span'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[2]'), "date": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[3]/small')}); index += 1
                
            index = 1;
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[9]/div/div/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["certifications"].append({"profile_id": profile_id, "certification_index": index - 1, "name": extract_with_xpath(tree, f'{base_xpath}/div/div/div[1]'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]'), "link": extract_with_xpath(tree, f'{base_xpath}/div/div/div[4]/a/span')}); index += 1

        self.processed_count += 1

    def save_to_csv(self):
        """Saves the collected data into separate CSV files."""
        logger.info(f"Saving processed data to: {self.output_path}")
        for name, data in self.data_frames.items():
            if data:
                df = pd.DataFrame(data)
                output_file = os.path.join(self.output_path, f"{name}.csv")
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.info(f"  - Saved {len(df)} rows to {name}.csv")
            else:
                logger.warning(f"  - No data found for '{name}', CSV file will not be created.")

# --- Main Execution Function ---

def process_malt_data(limit: Optional[int] = None, include_ids: Optional[List[str]] = None):
    """
    Main function to run the entire ETL process for Malt data.

    :param limit: Optional integer to limit the number of profiles processed. Ignored if include_ids is set.
    :param include_ids: Optional list of specific profile IDs to process for targeted testing.
    """
    start_time = time.time()
    logger.info("====== Starting Malt Data ETL Process ======")

    # 1. Find and extract unique profiles from all raw JSON files
    json_files = find_and_load_json_files(Config.RAW_DATA_ROOT)
    if not json_files:
        logger.error("No raw JSON files found. Aborting process.")
        return

    all_profiles = extract_profiles_from_json(json_files)
    total_unique_profiles = len(all_profiles)
    profiles_to_process = {}
    run_type_info = {"type": "full_run", "details": "Processing all found profiles."}

    # 2. Filter profiles based on `include_ids` or `limit`
    if include_ids:
        # Prioritize include_ids: filter all_profiles to only those specified.
        logger.warning(f"Processing is limited to {len(include_ids)} specific profile IDs.")
        for profile_id in include_ids:
            if profile_id in all_profiles:
                profiles_to_process[profile_id] = all_profiles[profile_id]
            else:
                logger.warning(f"Requested profile ID '{profile_id}' not found in raw data.")
        run_type_info = {"type": "include_ids_run", "ids_requested": len(include_ids), "ids_found": len(profiles_to_process)}
    elif limit:
        # If no include_ids, apply the numeric limit.
        logger.warning(f"Processing is limited to {limit} profiles for testing.")
        profiles_to_process = {k: all_profiles[k] for k in list(all_profiles.keys())[:limit]}
        run_type_info = {"type": "limit_run", "limit_set": limit}
    else:
        # If neither is provided, process all profiles.
        profiles_to_process = all_profiles

    # 3. Set up output directory for this specific run
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_run_path = os.path.join(Config.PROCESSED_DATA_ROOT, run_timestamp)
    os.makedirs(output_run_path, exist_ok=True)
    logger.info(f"Output for this run will be saved in: {output_run_path}")

    # 4. Initialize and run the processor
    processor = MaltDataProcessor(output_run_path)
    logger.info(f"Starting processing for {len(profiles_to_process)} profiles...")

    for profile_id, json_data in tqdm(profiles_to_process.items(), desc="Processing Profiles"):
        processor.process_profile(profile_id, json_data)

    # 5. Save the processed data to CSV files
    processor.save_to_csv()

    # 6. Create metadata and success file for the run
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    
    run_metadata = {
        "run_timestamp_utc": run_timestamp,
        "duration_seconds": duration,
        "source_json_root": Config.RAW_DATA_ROOT,
        "source_html_root": Config.HTML_ROOT,
        "output_path": output_run_path,
        "total_unique_profiles_found": total_unique_profiles,
        "processing_run_type": run_type_info,
        "profiles_processed_in_run": processor.processed_count,
        "profiles_with_errors": processor.error_count,
        "data_schema_tables": Config.TABLE_NAMES
    }
    
    metadata_file_path = os.path.join(output_run_path, "metadata.json")
    with open(metadata_file_path, 'w', encoding='utf-8') as f:
        json.dump(run_metadata, f, indent=2)

    success_file_path = os.path.join(output_run_path, "_SUCCESS")
    with open(success_file_path, 'w') as f: pass

    logger.info(f"====== ETL Process Finished in {duration} seconds ======")
    logger.info(f"Successfully processed: {processor.processed_count} profiles.")
    logger.info(f"Encountered errors on: {processor.error_count} profiles.")

# --- Example Usage Block for Direct Execution ---
if __name__ == '__main__':
    # --- Example 1: Run for a limited number of profiles (e.g., 50) for a quick test.
    # process_malt_data(limit=50)

    # --- Example 2: Run for a specific list of profile IDs for targeted debugging.
    # The 'limit' argument will be ignored if 'include_ids' is provided.
    specific_ids_to_test = [
        "5d70fee8b7d0a40009ce9e00", # Example ID from the prompt
        "another_profile_id_to_test"   # Add other specific IDs here
    ]
    process_malt_data(include_ids=specific_ids_to_test)

    # --- Example 3: Run for all profiles (default behavior).
    # process_malt_data()