"""
File: src/scrapers/profile_ids_scraper.py
Author: [Your Name]
Date: [Current Date]

Description:
-------------
This module defines the ProfileIDScraper class, which is responsible for scraping caregiver 
profile IDs from the care.fr search pages. The scraper is implemented using a class-based design 
to encapsulate functionality. It accepts query parameters (such as postal code, care type, results 
per page, and base URL) via the constructor. The module uses regular expressions to extract profile 
IDs and the total number of search results from the HTML response. It also captures metadata such 
as the total duration of the scraping process, the number of pages scraped, and the scrape timestamp.
Finally, the results are saved as a JSON file under the directory structure:
    data/raw_data/France/<postal_code>/search_of_<care_type>/profileIDs.json

Dependencies:
-------------
- re, json, os, time, datetime, requests
- The logger utility from src/utils/logger.py is used for logging purposes.

Usage:
------
Run the module as a script to perform the scraping:
    python -m src.scrapers.profile_ids_scraper

Future Development:
-------------------
- Adjust or extend the regular expressions as needed if the HTML structure changes.
- Consider using Selenium if JavaScript rendering becomes necessary.
- Enhance error handling and rate limiting if scraping scale increases.
"""

import re
import json
import os
import time
from datetime import datetime
import requests
import random

# Append parent directory to sys.path so that we can import modules from utils
import sys
sys.path.append('..')
from utils.logger import get_logger  # Import our custom logger

class ProfileIDScraper:
    def __init__(
        self,
        postal_code: str = "75001",
        city: str = "Paris",
        care_type: str = "childCare",  # e.g., "childCare", "auPair", etc.
        subVerticalIdList: str = "",
        results_per_page: int = 50,
        base_url: str = "https://www.care.com/fr-fr/profiles",
        auth_token: str = None
    ):
        """
        Initialize the scraper with query parameters.
        
        Args:
            postal_code (str): The postal code to search in (default "75001").
            care_type (str): The type of care service (vertical ID) to search (default "childCare").
            results_per_page (int): The maximum number of results per page (default 50).
            base_url (str): The base URL for the search endpoint.
            auth_token (str): Optional authentication token or cookie string.
        """
        self.postal_code = postal_code
        self.city = city  # Assuming city is provided as part of postal code for simplicity.
        self.care_type = care_type
        self.subVerticalIdList = subVerticalIdList
        self.results_per_page = results_per_page
        self.base_url = base_url
        self.auth_token = auth_token
        self.logger = get_logger(self.__class__.__name__)
        self.session = requests.Session()
        if self.auth_token:
            # Update session headers if an authentication token is provided.
            self.session.headers.update({"Cookie": self.auth_token})
    
    def build_search_url(self, offset: int) -> str:
        """
        Constructs the search URL using the given pagination offset and stored query parameters.
        
        Args:
            offset (int): The pagination offset.
        
        Returns:
            str: The fully constructed search URL.
        """
        geo_region_id = f"POSTCODE-FR-{self.postal_code}"
        geo_region_search = f"{self.postal_code}"
        sub_type = ""
        if not(self.subVerticalIdList == ""):
            sub_type = f"&subVerticalIdList={self.subVerticalIdList}"
        url = (
            f"{self.base_url}?sort=bestMatch&order=asc&isRefinedSearch=true"
            f"&verticalId={self.care_type}&radius=50"
            f"&geoRegionId={geo_region_id}&geoRegionSearch={geo_region_search}"
            f"&max={self.results_per_page}&offset={offset}&id=pagination"
            f"{sub_type}"
        )
        return url

    def extract_total_results(self, html: str) -> int:
        """
        Extracts the total number of results from the HTML content.
        
        Expected pattern example in HTML: "407 Garde d'enfant résultats"
        
        Args:
            html (str): The HTML content from the search page.
        
        Returns:
            int: The total number of search results; 0 if not found.
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
        Extracts caregiver profile IDs from the HTML content using a regular expression.
        
        The regex looks for patterns like:
            class="entityId" name="entityId" value="9138062"
        
        Args:
            html (str): The HTML content from which to extract profile IDs.
        
        Returns:
            list: A list of profile ID strings.
        """
        pattern = re.compile(r'class="entityId" name="entityId" value="(\d+)"')
        ids = pattern.findall(html)
        self.logger.debug(f"Extracted IDs: {ids}")
        return ids
    
    def save_profile_ids(self, metadata: dict, profile_ids: list):
        """
        Saves the collected profile IDs along with scraping metadata to a JSON file.
        
        The output is saved under:
            data/raw_data/France/<postal_code>/search_of_<care_type>/profileIDs.json
        
        Args:
            metadata (dict): Metadata information such as scrape time and duration.
            profile_ids (list): List of extracted profile IDs.
        """
        output_dir = os.path.join("../data", "raw_data", "France", self.postal_code, f"search_of_{self.care_type}__{self.subVerticalIdList}")
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
        Main method to perform the scraping:
            - Iterates over search result pages based on pagination.
            - Extracts the total number of results and profile IDs.
            - Captures the total duration of the scraping process.
            - Saves all extracted IDs along with metadata to a JSON file.
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
            time.sleep(round(random.uniform(0.8, 1.2), 4))
        
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
    # Instantiate the scraper with default parameters (override if necessary).
    scraper = ProfileIDScraper(
        postal_code="75001",
        care_type="childCare",
        results_per_page=50,
        base_url="https://www.care.com/fr-fr/profiles",
        auth_token=None  # Replace with your token string if authentication is needed.
    )
    scraper.scrape()
