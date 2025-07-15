"""
Malt Data Processor (ETL): Raw to Structured CSV

PURPOSE:
  1) This script performs an ETL process on the raw data scraped from Malt.fr.
  2) It reads profile data from two primary sources:
     - Initial search API results (JSON files).
     - Stored, static profile pages (HTML files).
  3) It parses, cleans, and consolidates data from these sources using a predefined
     map of XPaths to extract specific fields from the static HTML.
  4) It structures the data into a relational schema and loads the final, clean data
     into separate CSV files in a 'processed_data' directory.
  5) This script follows DataOps principles by creating run-specific output folders
     and generating metadata for data lineage and processing transparency.

VERSION: 3.0.0 (Stable release using a pure XPath-based approach for all HTML extraction)

PREREQUISITES:
  - `pip install pandas lxml tqdm`

USAGE:
  - This script can be imported and run from a Jupyter Notebook or another Python script.
  - Call the main function `process_malt_data()`.
  - Optional `limit` and `include_ids` arguments allow for targeted test runs.

Author: @AyoubFrihaoui
Version: 3.0.0
"""

import os
import re
import json
import logging
import time
import pandas as pd
from lxml import html
from tqdm import tqdm
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

# --- Configuration ---
class Config:
    """
    Configuration class to hold all key paths and settings for the ETL process.
    Centralizing paths here makes the script easier to manage and adapt.
    """
    # Path to the root directory where the raw JSON search results are stored.
    RAW_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\raw_data"

    # Path to the root directory where the individual raw HTML profile pages are stored.
    HTML_ROOT = r"H:\Data Extraction FSP\Malt-fr\raw_data\malt_fr\developpeur\en_télétravail\profiles\developpeur\en_télétravail\profiles"

    # Path to the root directory where the final processed CSV files will be saved.
    PROCESSED_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\processed_data"

    # Defines the names of all output CSV files. This list drives the data structuring.
    TABLE_NAMES = [
        "profiles", "profile_skills", "profile_badges", "profile_languages",
        "profile_categories", "portfolio_items", "reviews", "recommendations",
        "experiences", "education", "certifications"
    ]

# --- Logging Setup ---
# Configures a global logger to provide clear, formatted output.
logging.basicConfig(
    level=logging.INFO, # Set minimum level of messages to display.
    format="[%(asctime)s] %(levelname)s - %(message)s", # Define the log message format.
    datefmt="%Y-%m-%d %H:%M:%S" # Define the timestamp format.
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    """Safely retrieve a nested value from a dictionary to avoid KeyError."""
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default # Return default value if any key is missing.
    return data

def find_and_load_json_files(root_path: str) -> List[str]:
    """Recursively finds all 'page_*.json' files in the specified root path."""
    json_files = []
    logger.info(f"Searching for raw JSON files in: {root_path}")
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            if filename.startswith("page_") and filename.endswith(".json"):
                json_files.append(os.path.join(dirpath, filename))
    logger.info(f"Found {len(json_files)} raw JSON files to process.")
    return json_files

def extract_profiles_from_json(json_files: List[str]) -> Dict[str, Dict]:
    """Extracts unique profiles from a list of JSON files, using profile ID as the key."""
    profiles = {}
    logger.info("Extracting unique profiles from JSON files...")
    # Use tqdm for a progress bar, which is helpful for long-running tasks.
    for file_path in tqdm(json_files, desc="Reading JSON files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # A single JSON file can contain multiple profiles.
            for profile_data in data.get("profiles", []):
                profile_id = profile_data.get("id")
                # Add profile to dictionary only if it's new, ensuring uniqueness.
                if profile_id and profile_id not in profiles:
                    profiles[profile_id] = profile_data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read or parse {file_path}: {e}")
    logger.info(f"Extracted {len(profiles)} unique profiles.")
    return profiles

# --- HTML Parsing Functions ---
def extract_with_xpath(tree: html.HtmlElement, xpath: str) -> Optional[str]:
    """A robust wrapper for lxml's xpath method to extract single text element."""
    if not xpath: return None # Guard against empty xpath strings
    try:
        elements = tree.xpath(xpath)
        # Return cleaned text content if element is found.
        if elements:
            return elements[0].text_content().strip()
    except Exception as e:
        logger.debug(f"XPath evaluation failed for '{xpath}': {e}")
    return None

def extract_list_with_xpath(tree: html.HtmlElement, xpath_template: str, limit=150) -> List[str]:
    """Extracts a list of text content by iterating an indexed XPath template."""
    items = []
    # Loop from 1 up to a reasonable limit to avoid infinite loops on malformed pages.
    for index in range(1, limit + 1):
        # Format the XPath with the current index.
        xpath = xpath_template.format(index=index)
        element_text = extract_with_xpath(tree, xpath)
        if element_text:
            items.append(element_text)
        else:
            # Stop when no more elements are found at the current index.
            break
    return items

def extract_nested_list_with_xpath(tree: html.HtmlElement, xpath_template: str, limit=10) -> List[str]:
    """Extracts a list of text from a nested structure (e.g., categories)."""
    items = []
    for outer_index in range(1, limit + 1):
        found_in_outer = False
        for inner_index in range(1, limit + 1):
            xpath = xpath_template.format(index_cat=outer_index, index_item=inner_index)
            element_text = extract_with_xpath(tree, xpath)
            if element_text:
                items.append(element_text)
                found_in_outer = True
            else:
                break # Break inner loop
        if not found_in_outer:
            break # Break outer loop
    return items


# --- Main Processor Class ---
class MaltDataProcessor:
    """Orchestrates the ETL process from raw files to structured CSVs."""
    def __init__(self, output_run_path: str):
        # Initialize a dictionary of lists to hold data for each table.
        self.data_frames: Dict[str, List[Dict]] = {name: [] for name in Config.TABLE_NAMES}
        # A single timestamp for the entire processing run, for data lineage.
        self.retrieved_at = datetime.now(timezone.utc).isoformat()
        self.output_path = output_run_path
        self.processed_count = 0
        self.error_count = 0

    def process_profile(self, profile_id: str, json_data: Dict):
        """Processes a single profile, combining data from JSON and its corresponding HTML file."""
        html_file_path = os.path.join(Config.HTML_ROOT, f"{profile_id}.html")
        tree = None
        if os.path.exists(html_file_path):
            try:
                # Read and parse the static HTML file using lxml for fast XPath evaluation.
                with open(html_file_path, 'r', encoding='utf-8') as f:
                    tree = html.fromstring(f.read())
            except (IOError, html.etree.ParserError) as e:
                logger.warning(f"Could not read or parse HTML for profile {profile_id}: {e}")
                self.error_count += 1
                return # Skip this profile if its HTML is corrupted.
        else:
            logger.debug(f"HTML file not found for profile {profile_id}. Some data will be missing.")

        # --- 1. PROFILES TABLE ---
        # Data for this main table comes from the initial Search API JSON and is enriched with HTML data.
        profile_row = {
            "profile_id": profile_id,
            "first_name": json_data.get('firstName'),
            "last_name": None, # Will be populated from HTML if possible
            "headline": json_data.get('headline'),
            "city": safe_get(json_data, ['location', 'city']),
            "location_type": safe_get(json_data, ['location', 'locationType']),
            "price_amount": safe_get(json_data, ['price', 'value', 'amount']),
            "price_currency": safe_get(json_data, ['price', 'value', 'currency']),
            "availability_status": safe_get(json_data, ['availability', 'status']),
            "work_availability": safe_get(json_data, ['availability', 'workAvailability']),
            "next_availability_date": safe_get(json_data, ['availability', 'nextAvailabilityDate']),
            "stats_missions_count": safe_get(json_data, ['stats', 'missionsCount']),
            "stats_recommendations_count": safe_get(json_data, ['stats', 'recommendationsCount']),
            "retrieved_at": self.retrieved_at,
        }

        # Enrich the profile row with data extracted from the parsed HTML tree.
        if tree is not None:
            # Extract single-value fields using XPath.
            profile_row.update({
                "experience_years_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[2]/span[2]'),
                "response_rate_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[3]/span[2]'),
                "response_time_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[4]/span[2]'),
                "profile_description": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[3]/div/div/div'),
                "has_signed_charter": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[1]/div/div[2]/p')),
                "has_verified_email": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[4]/div/div[2]')),
            })
        self.data_frames["profiles"].append(profile_row)

        # --- One-to-Many Tables ---
        # These tables store data where one profile can have multiple entries (e.g., multiple skills).
        # These come from the initial JSON data.
        for skill in json_data.get('skills', []): self.data_frames["profile_skills"].append({"profile_id": profile_id, "skill_name": skill.get('label'), "is_certified": skill.get('certified')})
        for badge in json_data.get('badges', []): self.data_frames["profile_badges"].append({"profile_id": profile_id, "badge_name": badge})
        for item in json_data.get('portfolio', []): self.data_frames["portfolio_items"].append({"profile_id": profile_id, "item_index": item.get('index'), "title": item.get('title'), "item_type": item.get('type'), "low_res_url": safe_get(item, ['picture', 'lowResolutionUrl']), "med_res_url": safe_get(item, ['picture', 'mediumResolutionUrl']), "high_res_url": safe_get(item, ['picture', 'highResolutionUrl'])})
        
        # Process tables that can only be extracted from the HTML file.
        if tree is not None:
            # Languages
            for lang in extract_list_with_xpath(tree, '//*[@id="languages-section"]/div/ul/li[{index}]/p[1]'): self.data_frames["profile_languages"].append({"profile_id": profile_id, "language_name": lang})
            # Categories
            for cat in extract_nested_list_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[4]/div/div[{index_cat}]/ul/li[{index_item}]/a/span'): self.data_frames["profile_categories"].append({"profile_id": profile_id, "category_name": cat})
            
            # Recommendations
            for index in range(3, 151): # Safety limit
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[7]/div/div/div[{index}]'
                if not tree.xpath(base_xpath): break
                name = extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/span[2]') or extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/a/span/span[2]')
                self.data_frames["recommendations"].append({"profile_id": profile_id, "recommendation_index": index - 3, "recommender_name": name, "recommender_company": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[2]'), "recommendation_date": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[2]'), "recommendation_desc": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[3]')})
            
            # Experiences
            for index in range(1, 151): # Safety limit
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[5]/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["experiences"].append({"profile_id": profile_id, "experience_index": index - 1, "company_name": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[1]/div'), "title": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[2]'), "date_range": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[1]/span'), "location": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[2]'), "description": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[4]/div')})

            # Education
            for index in range(1, 151): # Safety limit
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[8]/div/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["education"].append({"profile_id": profile_id, "education_index": index - 1, "degree": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[1]/span'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[2]'), "date": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[3]/small')})
                
            # Certifications
            for index in range(1, 151): # Safety limit
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[9]/div/div/div/ul/li[{index}]'
                if not tree.xpath(base_xpath): break
                self.data_frames["certifications"].append({"profile_id": profile_id, "certification_index": index - 1, "name": extract_with_xpath(tree, f'{base_xpath}/div/div/div[1]'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]'), "link": extract_with_xpath(tree, f'{base_xpath}/div/div/div[4]/a/span')})

            # Reviews - using the newly provided XPath map
            for index in range(2, 151): # Safety limit
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[6]/div/div[2]/div[{index}]/article'
                #base_xpatt = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[6]/div/div[2]/div[{index}]/article/'
                if not tree.xpath(base_xpath): break
                self.data_frames["reviews"].append({
                    "profile_id": profile_id,
                    "review_index": index - 2,
                    "name": extract_with_xpath(tree, f'{base_xpath}/div[2]/div/div/h2/span[1]'),
                    "company": extract_with_xpath(tree, f'{base_xpath}/div[2]/div/div/h2/span[3]'), # Corrected typo 'ml/body'
                    "date": extract_with_xpath(tree, f'{base_xpath}/div[2]/div/div/p'),
                    "description": extract_with_xpath(tree, f'{base_xpath}/div[2]/p')
                })

        self.processed_count += 1

    def save_to_csv(self):
        """Saves the collected data into separate CSV files, one for each table."""
        logger.info(f"Saving processed data to: {self.output_path}")
        for name in Config.TABLE_NAMES: # Iterate through defined table names to ensure all are handled
            data = self.data_frames.get(name, [])
            if data:
                df = pd.DataFrame(data)
                output_file = os.path.join(self.output_path, f"{name}.csv")
                # Use utf-8-sig encoding for better compatibility with Excel (handles BOM).
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
    logger.info("====== Starting Malt Data ETL Process (XPath Parser v3.0) ======")

    # Step 1: Find and extract unique profiles from all raw JSON files
    json_files = find_and_load_json_files(Config.RAW_DATA_ROOT)
    if not json_files: logger.error("No raw JSON files found. Aborting process."); return
    all_profiles = extract_profiles_from_json(json_files)
    total_unique_profiles = len(all_profiles)
    profiles_to_process = {}
    run_type_info = {"type": "full_run", "details": "Processing all found profiles."}

    # Step 2: Filter profiles based on `include_ids` or `limit` for testing
    if include_ids:
        logger.warning(f"Processing is limited to {len(include_ids)} specific profile IDs.")
        for pid in include_ids:
            if pid in all_profiles: profiles_to_process[pid] = all_profiles[pid]
            else: logger.warning(f"Requested profile ID '{pid}' not found in raw data.")
        run_type_info = {"type": "include_ids_run", "ids_requested": len(include_ids), "ids_found": len(profiles_to_process)}
    elif limit:
        logger.warning(f"Processing is limited to {limit} profiles for testing."); profiles_to_process = {k: all_profiles[k] for k in list(all_profiles.keys())[:limit]};
        run_type_info = {"type": "limit_run", "limit_set": limit}
    else: profiles_to_process = all_profiles
    if not profiles_to_process: logger.error("No profiles to process after filtering. Aborting."); return

    # Step 3: Set up a unique, timestamped output directory for this run
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"); output_run_path = os.path.join(Config.PROCESSED_DATA_ROOT, run_timestamp); os.makedirs(output_run_path, exist_ok=True)
    logger.info(f"Output for this run will be saved in: {output_run_path}")

    # Step 4: Initialize and run the processor
    processor = MaltDataProcessor(output_run_path); logger.info(f"Starting processing for {len(profiles_to_process)} profiles...")
    for profile_id, json_data in tqdm(profiles_to_process.items(), desc="Processing Profiles"):
        processor.process_profile(profile_id, json_data)

    # Step 5: Save the processed dataframes to CSV files
    processor.save_to_csv()

    # Step 6: Create metadata and a _SUCCESS marker file for the run
    end_time = time.time(); duration = round(end_time - start_time, 2)
    run_metadata = {
        "run_timestamp_utc": run_timestamp, "duration_seconds": duration, "source_json_root": Config.RAW_DATA_ROOT,
        "source_html_root": Config.HTML_ROOT, "output_path": output_run_path, "total_unique_profiles_found": total_unique_profiles,
        "processing_run_type": run_type_info, "profiles_processed_in_run": processor.processed_count,
        "profiles_with_errors": processor.error_count, "data_schema_tables": Config.TABLE_NAMES
    }
    with open(os.path.join(output_run_path, "metadata.json"), 'w', encoding='utf-8') as f: json.dump(run_metadata, f, indent=2)
    with open(os.path.join(output_run_path, "_SUCCESS"), 'w') as f: pass

    logger.info(f"====== ETL Process Finished in {duration} seconds ======"); logger.info(f"Successfully processed: {processor.processed_count} profiles."); logger.info(f"Encountered errors on: {processor.error_count} profiles.")

# --- Example Usage Block for Direct Execution ---
if __name__ == '__main__':
    process_malt_data(include_ids=["5d70fee8b7d0a40009ce9e00"])