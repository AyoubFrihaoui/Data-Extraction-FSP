# src/scrapers/profile_ids_scraper.py

import re
import json
import os
import time
from datetime import datetime
import requests

import sys
sys.path.append('..')
from utils.logger import get_logger

class ProfileIDScraper:
    def __init__(
        self,
        postal_code: str = "75001",
        care_type: str = "childCare",  # e.g., "childCare", "auPair", etc.
        results_per_page: int = 50,
        base_url: str = "https://www.care.com/fr-fr/profiles",
        auth_token: str = None
    ):
        """
        Initialize the scraper with query parameters.
        Query parameters are now provided via the constructor.
        """
        self.postal_code = postal_code
        self.care_type = care_type
        self.results_per_page = results_per_page
        self.base_url = base_url
        self.auth_token = auth_token
        self.logger = get_logger(self.__class__.__name__)
        self.session = requests.Session()
        if self.auth_token:
            self.session.headers.update({"Cookie": self.auth_token})
    
    def build_search_url(self, offset: int) -> str:
        """
        Constructs the search URL using query parameters and the pagination offset.
        """
        geo_region_id = f"POSTCODE-FR-{self.postal_code}"
        geo_region_search = f"{self.postal_code}+Paris"
        url = (
            f"{self.base_url}?sort=bestMatch&order=asc&isRefinedSearch=true"
            f"&verticalId={self.care_type}&radius=30"
            f"&geoRegionId={geo_region_id}&geoRegionSearch={geo_region_search}"
            f"&max={self.results_per_page}&offset={offset}&id=pagination"
        )
        return url
    
    def extract_total_results(self, html: str) -> int:
        """
        Extracts the total number of results from the HTML.
        Expected pattern example: "407 Garde d'enfant résultats"
        """
        total_pattern = re.compile(r'(\d+)\s+Garde d\'enfant résultats')
        match = total_pattern.search(html)
        if match:
            total = int(match.group(1))
            self.logger.debug(f"Total results extracted: {total}")
            return total
        else:
            self.logger.warning("Total results not found in HTML.")
            return 0
    
    def extract_profile_ids(self, html: str) -> list:
        """
        Extracts profile IDs using a regex that matches:
        class="entityId" name="entityId" value="9138062"
        """
        pattern = re.compile(r'class="entityId" name="entityId" value="(\d+)"')
        ids = pattern.findall(html)
        self.logger.debug(f"Extracted IDs: {ids}")
        return ids
    
    def save_profile_ids(self, metadata: dict, profile_ids: list):
        """
        Saves the collected profile IDs and associated metadata to a JSON file.
        Output path: data/raw_data/France/<postal_code>/search_of_<care_type>/profileIDs.json
        """
        output_dir = os.path.join("../data", "raw_data", "France", self.postal_code, f"search_of_{self.care_type}")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "profileIDs.json")
        
        data = {
            "metadata": metadata,
            "profileIDs": profile_ids
        }
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved {len(profile_ids)} profile IDs to {output_file}")
    
    def scrape(self):
        """
        Main method to scrape all pages for the specified query, extract profile IDs,
        and store metadata including the duration of the scraping process.
        """
        start_time = time.time()
        all_profile_ids = []
        total_results = None
        page = 0
        offset = 0
        
        while True:
            url = self.build_search_url(offset)
            self.logger.info(f"Fetching URL: {url}")
            try:
                response = self.session.get(url)
                response.raise_for_status()
                html = response.text
            except Exception as e:
                self.logger.error(f"Error fetching URL {url}: {e}")
                break
            
            if total_results is None:
                total_results = self.extract_total_results(html)
                if total_results:
                    self.logger.info(f"Total results found: {total_results}")
                else:
                    self.logger.warning("Proceeding without total results info.")
            
            profile_ids = self.extract_profile_ids(html)
            if not profile_ids:
                self.logger.info("No profile IDs found on this page. Ending pagination.")
                break
            
            self.logger.info(f"Page {page + 1}: Found {len(profile_ids)} profile IDs.")
            all_profile_ids.extend(profile_ids)
            page += 1
            offset = page * self.results_per_page
            
            if total_results and len(all_profile_ids) >= total_results:
                self.logger.info("Scraped all expected profile IDs.")
                break
            
            # Introduce a delay to avoid rate limiting
            time.sleep(1 + 0.5 * page)
        
        duration = time.time() - start_time
        metadata = {
            "postal_code": self.postal_code,
            "care_type": self.care_type,
            "scrape_time": datetime.utcnow().isoformat() + "Z",
            "total_results": total_results if total_results else len(all_profile_ids),
            "pages_scraped": page,
            "duration_seconds": duration
        }
        self.logger.info(f"Scraping completed in {duration:.2f} seconds, scraped {len(all_profile_ids)} profile IDs.")
        self.save_profile_ids(metadata, all_profile_ids)

if __name__ == "__main__":
    # You can override default values here if needed.
    scraper = ProfileIDScraper(
        postal_code="75001",
        care_type="childCare",
        results_per_page=50,
        base_url="https://www.care.com/fr-fr/profiles",
        auth_token=None  # Replace with token string if necessary.
    )
    scraper.scrape()
