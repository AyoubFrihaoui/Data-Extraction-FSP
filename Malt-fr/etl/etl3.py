"""
Malt Data Processor (ETL): Raw to Structured CSV

PURPOSE:
  (Same as previous version)

VERSION: 1.2.1 (Added robust Selenium handling: better waits, cookie handling, and debug screenshots)

PREREQUISITES:
  - `pip install selenium pandas lxml tqdm`
  - A WebDriver (e.g., chromedriver.exe) must be downloaded and its path provided.
    https://googlechromelabs.github.io/chrome-for-testing/

Author: @AyoubFrihaoui
Version: 1.2.1
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

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- Configuration ---
class Config:
    RAW_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\raw_data"
    # NOTE: The old HTML_ROOT is no longer needed as we fetch live HTML.
    PROCESSED_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\processed_data"
    
    # !!! IMPORTANT: Update this path to where you saved your chromedriver.exe
    CHROMEDRIVER_PATH = r"C:/chromedriver-win64/chromedriver.exe"  # Make sure this path is correct
    
    TABLE_NAMES = [
        "profiles", "profile_skills", "profile_badges", "profile_languages",
        "profile_categories", "portfolio_items", "reviews", "recommendations",
        "experiences", "education", "certifications"
    ]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# --- Helper Functions (unchanged) ---
def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data: data = data[key]
        else: return default
    return data

def find_and_load_json_files(root_path: str) -> List[str]:
    json_files = []; logger.info(f"Searching for raw JSON files in: {root_path}")
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            if filename.startswith("page_") and filename.endswith(".json"): json_files.append(os.path.join(dirpath, filename))
    logger.info(f"Found {len(json_files)} raw JSON files."); return json_files

def extract_profiles_from_json(json_files: List[str]) -> Dict[str, Dict]:
    profiles = {}; logger.info("Extracting unique profiles from JSON files...")
    for file_path in tqdm(json_files, desc="Reading JSON files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
            for profile_data in data.get("profiles", []):
                profile_id = profile_data.get("id")
                if profile_id and profile_id not in profiles: profiles[profile_id] = profile_data
        except (json.JSONDecodeError, IOError) as e: logger.warning(f"Could not read or parse {file_path}: {e}")
    logger.info(f"Extracted {len(profiles)} unique profiles."); return profiles

# --- HTML Parsing Functions (unchanged) ---
def parse_ld_json(html_content: str) -> Optional[Dict]:
    ld_json_scripts = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL)
    if len(ld_json_scripts) > 1:
        try: return json.loads(ld_json_scripts[1])
        except json.JSONDecodeError: return None
    return None

def extract_with_xpath(tree: html.HtmlElement, xpath: str) -> Optional[str]:
    elements = tree.xpath(xpath)
    if elements: return elements[0].text_content().strip()
    return None

def extract_list_with_xpath(tree: html.HtmlElement, xpath_template: str) -> List[str]:
    items = []; index = 1
    while True:
        xpath = xpath_template.format(index=index); element = extract_with_xpath(tree, xpath)
        if element: items.append(element); index += 1
        else: break
    return items

def extract_nested_list_with_xpath(tree: html.HtmlElement, xpath_template: str) -> List[str]:
    items = []; outer_index = 1
    while True:
        inner_index = 1; found_in_outer = False
        while True:
            xpath = xpath_template.format(index_cat=outer_index, index_item=inner_index); element = extract_with_xpath(tree, xpath)
            if element: items.append(element); inner_index += 1; found_in_outer = True
            else: break
        if not found_in_outer: break
        outer_index += 1
    return items

# --- Main Processor Class ---
class MaltDataProcessor:
    """Orchestrates the ETL process using Selenium for dynamic content."""
    def __init__(self, output_run_path: str):
        self.data_frames: Dict[str, List[Dict]] = {name: [] for name in Config.TABLE_NAMES}
        self.retrieved_at = datetime.now(timezone.utc).isoformat()
        self.output_path = output_run_path
        self.processed_count = 0; self.error_count = 0
        self.driver = self._init_selenium()

    def _init_selenium(self) -> webdriver.Chrome:
        logger.info("Initializing Selenium WebDriver...")
        service = Service(executable_path=Config.CHROMEDRIVER_PATH)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new") # Use new headless mode
        options.add_argument("--disable-gpu"); options.add_argument("--no-sandbox")
        options.add_argument("--log-level=3"); options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        return webdriver.Chrome(service=service, options=options)

    def close_selenium(self):
        if self.driver: logger.info("Closing Selenium WebDriver."); self.driver.quit()

    def get_full_page_html(self, profile_id: str, profile_url: str) -> Optional[str]:
        """Uses Selenium to get fully rendered HTML, clicking "Show more" buttons."""
        try:
            self.driver.get(profile_url)
            
            # <<< CHANGED: More specific and longer wait >>>
            # Wait for the main H1 title of the profile to be present, which is a good sign the page has loaded.
            # Increased timeout to 30 seconds.
            main_element_xpath = '//h1[@data-testid="freelance-header-title"]'
            WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.XPATH, main_element_xpath)))
            
            # <<< NEW: Handle cookie banner >>>
            try:
                cookie_button = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                cookie_button.click(); logger.debug("Accepted cookie banner.")
                time.sleep(0.5) # Pause for banner to disappear
            except TimeoutException:
                logger.debug("Cookie banner not found or already accepted.")

            # --- Click "Show more experiences" button ---
            try:
                show_more_button_xpath = "/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[5]/div/div/button"
                show_more_button = self.driver.find_element(By.XPATH, show_more_button_xpath)
                self.driver.execute_script("arguments[0].scrollIntoView(true);", show_more_button)
                time.sleep(0.5); show_more_button.click(); logger.debug(f"Clicked 'Show more experiences' for {profile_url}")
                time.sleep(2) # Wait for content to load
            except NoSuchElementException: logger.debug(f"'Show more experiences' button not found for {profile_url}.")
            except Exception as e: logger.warning(f"Could not click 'Show more experiences' button for {profile_url}: {e}")

            return self.driver.page_source

        except TimeoutException:
            logger.error(f"Timeout (30s) waiting for main element on page: {profile_url}")
            # <<< NEW: Save screenshot on timeout for debugging >>>
            screenshot_path = os.path.join(self.output_path, f"timeout_error_{profile_id}.png")
            try:
                self.driver.save_screenshot(screenshot_path)
                logger.error(f"Saved timeout screenshot to: {screenshot_path}")
            except Exception as e: logger.error(f"Failed to save timeout screenshot: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected Selenium error occurred for {profile_url}: {e}")
            return None

    def process_profile(self, profile_id: str, json_data: Dict):
        """Processes a single profile, fetching live HTML with Selenium."""
        profile_url_from_search = safe_get(json_data, ['url'])
        if not profile_url_from_search:
            logger.warning(f"No URL in JSON for profile {profile_id}. Skipping."); self.error_count += 1; return

        full_profile_url = f"https://www.malt.fr{profile_url_from_search}"
        
        # <<< CHANGED: Pass profile_id to get_full_page_html for screenshot naming >>>
        html_content = self.get_full_page_html(profile_id, full_profile_url)
        
        if not html_content:
            logger.error(f"Failed to retrieve HTML for profile {profile_id}. Skipping."); self.error_count += 1; return

        ld_json_data = {}; tree = None
        try:
            tree = html.fromstring(html_content)
            parsed_ld = parse_ld_json(html_content)
            if parsed_ld: ld_json_data = parsed_ld
            else: logger.warning(f"Could not find or parse ld+json schema for {profile_id}")
        except html.etree.ParserError as e:
            logger.error(f"Could not parse HTML for profile {profile_id}: {e}"); self.error_count += 1; return

        # --- The rest of the processing logic remains the same ---
        # It now operates on the complete, fully-rendered HTML from Selenium.
        
        full_name = safe_get(ld_json_data, ['name'], ''); first_name = json_data.get('firstName', '')
        last_name = full_name.replace(first_name, '').strip() if full_name and first_name else None
        canonical_url = safe_get(ld_json_data, ['url'], full_profile_url)

        profile_row = {
            "profile_id": profile_id, "first_name": first_name, "last_name": last_name, "headline": json_data.get('headline'),
            "city": safe_get(json_data, ['location', 'city']), "location_type": safe_get(json_data, ['location', 'locationType']),
            "price_amount": safe_get(json_data, ['price', 'value', 'amount']), "price_currency": safe_get(json_data, ['price', 'value', 'currency']),
            "profile_url": canonical_url, "photo_url": safe_get(ld_json_data, ['image']),
            "availability_status": safe_get(json_data, ['availability', 'status']), "work_availability": safe_get(json_data, ['availability', 'workAvailability']),
            "next_availability_date": safe_get(json_data, ['availability', 'nextAvailabilityDate']),
            "stats_missions_count": safe_get(json_data, ['stats', 'missionsCount']), "stats_recommendations_count": safe_get(json_data, ['stats', 'recommendationsCount']),
            "aggregate_rating_value": safe_get(ld_json_data, ['aggregateRating', 'ratingValue']), "aggregate_rating_count": safe_get(ld_json_data, ['aggregateRating', 'ratingCount']),
            "aggregate_review_count": safe_get(ld_json_data, ['aggregateRating', 'reviewCount']), "aggregate_best_rating": safe_get(ld_json_data, ['aggregateRating', 'bestRating']),
            "aggregate_worst_rating": safe_get(ld_json_data, ['aggregateRating', 'worstRating']), "retrieved_at": self.retrieved_at,
        }
        if tree is not None:
            profile_row.update({
                "experience_years_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[2]/span[2]'),
                "response_rate_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[3]/span[2]'),
                "response_time_text": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[4]/span[2]'),
                "profile_description": extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[3]/div/div/div'),
                "has_signed_charter": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[1]/div/div[2]/p')),
                "has_verified_email": bool(extract_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[4]/div/div[2]')),
            })
        self.data_frames["profiles"].append(profile_row)

        for skill in json_data.get('skills', []): self.data_frames["profile_skills"].append({"profile_id": profile_id, "skill_name": skill.get('label'), "is_certified": skill.get('certified')})
        for badge in json_data.get('badges', []): self.data_frames["profile_badges"].append({"profile_id": profile_id, "badge_name": badge})
        for item in json_data.get('portfolio', []): self.data_frames["portfolio_items"].append({"profile_id": profile_id, "item_index": item.get('index'), "title": item.get('title'), "item_type": item.get('type'), "low_res_url": safe_get(item, ['picture', 'lowResolutionUrl']), "med_res_url": safe_get(item, ['picture', 'mediumResolutionUrl']), "high_res_url": safe_get(item, ['picture', 'highResolutionUrl'])})
        for i, review in enumerate(safe_get(ld_json_data, ['review'], [])): self.data_frames["reviews"].append({"profile_id": profile_id, "review_index": i, "author_name": safe_get(review, ['author', 'name']), "author_image_url": safe_get(review, ['author', 'image']), "date_published": review.get('datePublished'), "review_body": review.get('reviewBody')})
        
        if tree is not None:
            for lang in extract_list_with_xpath(tree, '//*[@id="languages-section"]/div/ul/li[{index}]/p[1]'): self.data_frames["profile_languages"].append({"profile_id": profile_id, "language_name": lang})
            for cat in extract_nested_list_with_xpath(tree, '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[4]/div/div[{index_cat}]/ul/li[{index_item}]/a/span'): self.data_frames["profile_categories"].append({"profile_id": profile_id, "category_name": cat})
            index = 1
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[7]/div/div/div[{index}]';
                if not tree.xpath(base_xpath): break
                name = extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/span[2]') or extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[1]/a/span/span[2]'); self.data_frames["recommendations"].append({"profile_id": profile_id, "recommendation_index": index - 1, "recommender_name": name, "recommender_company": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[1]/div[2]'), "recommendation_date": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[2]'), "recommendation_desc": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]/div[3]')}); index += 1
            index = 1
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[5]/div/ul/li[{index}]';
                if not tree.xpath(base_xpath): break
                self.data_frames["experiences"].append({"profile_id": profile_id, "experience_index": index - 1, "company_name": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[1]/div'), "title": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[2]'), "date_range": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[1]/span'), "location": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[3]/div[2]'), "description": extract_with_xpath(tree, f'{base_xpath}/div/div[2]/div/div[4]/div')}); index += 1
            index = 1
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[8]/div/div/ul/li[{index}]';
                if not tree.xpath(base_xpath): break
                self.data_frames["education"].append({"profile_id": profile_id, "education_index": index - 1, "degree": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[1]/span'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[2]'), "date": extract_with_xpath(tree, f'{base_xpath}/div/div/div/div[3]/small')}); index += 1
            index = 1
            while True:
                base_xpath = f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[9]/div/div/div/ul/li[{index}]';
                if not tree.xpath(base_xpath): break
                self.data_frames["certifications"].append({"profile_id": profile_id, "certification_index": index - 1, "name": extract_with_xpath(tree, f'{base_xpath}/div/div/div[1]'), "institution": extract_with_xpath(tree, f'{base_xpath}/div/div/div[2]'), "link": extract_with_xpath(tree, f'{base_xpath}/div/div/div[4]/a/span')}); index += 1
        self.processed_count += 1

    def save_to_csv(self):
        logger.info(f"Saving processed data to: {self.output_path}")
        for name, data in self.data_frames.items():
            if data:
                df = pd.DataFrame(data); output_file = os.path.join(self.output_path, f"{name}.csv")
                df.to_csv(output_file, index=False, encoding='utf-8-sig'); logger.info(f"  - Saved {len(df)} rows to {name}.csv")
            else: logger.warning(f"  - No data found for '{name}', CSV file will not be created.")

# --- Main Execution Function ---
def process_malt_data(limit: Optional[int] = None, include_ids: Optional[List[str]] = None):
    start_time = time.time(); logger.info("====== Starting Malt Data ETL Process (with Selenium) ======")
    json_files = find_and_load_json_files(Config.RAW_DATA_ROOT)
    if not json_files: logger.error("No raw JSON files found. Aborting."); return
    all_profiles = extract_profiles_from_json(json_files); total_unique_profiles = len(all_profiles); profiles_to_process = {};
    run_type_info = {"type": "full_run", "details": "Processing all found profiles."}

    if include_ids:
        logger.warning(f"Processing is limited to {len(include_ids)} specific profile IDs.")
        for pid in include_ids:
            if pid in all_profiles: profiles_to_process[pid] = all_profiles[pid]
            else: logger.warning(f"Requested profile ID '{pid}' not found in raw data.")
        run_type_info = {"type": "include_ids_run", "ids_requested": len(include_ids), "ids_found": len(profiles_to_process)}
    elif limit:
        logger.warning(f"Processing is limited to {limit} profiles."); profiles_to_process = {k: all_profiles[k] for k in list(all_profiles.keys())[:limit]};
        run_type_info = {"type": "limit_run", "limit_set": limit}
    else: profiles_to_process = all_profiles
    if not profiles_to_process: logger.error("No profiles to process after filtering. Aborting."); return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"); output_run_path = os.path.join(Config.PROCESSED_DATA_ROOT, run_timestamp); os.makedirs(output_run_path, exist_ok=True)
    logger.info(f"Output for this run will be saved in: {output_run_path}")
    processor = MaltDataProcessor(output_run_path); logger.info(f"Starting processing for {len(profiles_to_process)} profiles...")
    
    try:
        for profile_id, json_data in tqdm(profiles_to_process.items(), desc="Processing Profiles"): processor.process_profile(profile_id, json_data)
    finally:
        processor.close_selenium()

    processor.save_to_csv()
    end_time = time.time(); duration = round(end_time - start_time, 2)
    run_metadata = {
        "run_timestamp_utc": run_timestamp, "duration_seconds": duration, "source_json_root": Config.RAW_DATA_ROOT,
        "processing_mode": "Selenium (Live Fetch)", "output_path": output_run_path, "total_unique_profiles_found": total_unique_profiles,
        "processing_run_type": run_type_info, "profiles_processed_in_run": processor.processed_count, "profiles_with_errors": processor.error_count,
        "data_schema_tables": Config.TABLE_NAMES
    }
    with open(os.path.join(output_run_path, "metadata.json"), 'w', encoding='utf-8') as f: json.dump(run_metadata, f, indent=2)
    with open(os.path.join(output_run_path, "_SUCCESS"), 'w') as f: pass

    logger.info(f"====== ETL Process Finished in {duration} seconds ======"); logger.info(f"Successfully processed: {processor.processed_count} profiles."); logger.info(f"Encountered errors on: {processor.error_count} profiles.")

# --- Example Usage Block ---
if __name__ == '__main__':
    if not os.path.exists(Config.CHROMEDRIVER_PATH):
        logger.error(f"ChromeDriver not found at: {Config.CHROMEDRIVER_PATH}. Please update the path in etl.py")
    else:
        process_malt_data(include_ids=["5d70fee8b7d0a40009ce9e00"])