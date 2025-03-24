"""
File: src/monitors/top_caregivers_monitor.py
Author: @AyoubFrihaoui
Date: 03/17/2025

Description:
-------------
This module defines the TopCaregiversMonitor class, which monitors the top 20 caregiver profiles
for multiple care types (e.g., "childCare", "seniorCare") on the care.fr platform. The monitor runs
in a loop for a specified number of days, scraping the top 20 profiles for each care type every 
fixed interval (in hours).

For each care type, the monitor:
  - Constructs the search URL using the postal code and the current care type.
  - Scrapes the top 20 caregiver profile IDs from the search result page.
  - Saves the profile IDs along with metadata (scrape time, URL, HTTP status, etc.) into JSON files.
  
The output is saved under:
  data/monitoring/France/<postal_code>/monitor_of_<care_type>/
with filenames including a UTC timestamp to distinguish different monitoring cycles.

Usage:
------
Run the module as a script:
    python -m src.monitors.top_caregivers_monitor

Dependencies:
-------------
- Standard libraries: re, json, os, time, datetime, sys
- requests for HTTP requests.
- Logger utility from src/utils/logger.py for logging.
- __init__.py files must exist in src, src/monitors, and src/utils for proper package resolution.
"""

import re
import json
import os
import time
from datetime import datetime
import requests
import sys
from typing import Tuple, List

# Append parent directory to sys.path so that modules in utils can be imported.
sys.path.append('..')
from utils.logger import get_logger

class TopCaregiversMonitor:
    def __init__(self, postal_code: str, care_types: List[str], interval_hours: float, total_days: float, auth_token: str = None):
        """
        Initialize the monitor with search parameters and scheduling details.

        Args:
            postal_code (str): The postal code to search in (e.g., "75001").
            care_types (List[str]): A list of care types (vertical IDs) to monitor (e.g., ["childCare", "seniorCare"]).
            interval_hours (float): The interval between scrapes (in hours).
            total_days (float): Total monitoring duration in days.
            auth_token (str): Optional authentication token or cookie string.
        """
        self.postal_code = postal_code
        self.care_types = care_types  # List of care types to monitor.
        self.interval_hours = interval_hours
        self.total_days = total_days
        self.auth_token = auth_token
        
        # For monitoring, we always request the top 20 results.
        self.results_per_page = 20
        self.base_url = "https://www.care.com/fr-fr/profiles"
        
        self.logger = get_logger(self.__class__.__name__)
        self.session = requests.Session()
        if self.auth_token:
            self.session.headers.update({"Cookie": self.auth_token})
        
        # Base output folder for monitoring data:
        # data/monitoring/France/<postal_code>/
        self.base_output_folder = os.path.join(".." ,"data", "monitoring", "France", self.postal_code)
        os.makedirs(self.base_output_folder, exist_ok=True)
    
    def build_search_url(self, care_type: str) -> str:
        """
        Constructs the search URL for a given care type to fetch the top 20 caregiver profiles.
        
        Args:
            care_type (str): The care type (vertical ID) to search for.
            
        Returns:
            str: The constructed URL with offset fixed to 0 and max=20.
        """
        geo_region_id = f"POSTCODE-FR-{self.postal_code}"
        geo_region_search = f"{self.postal_code}+Paris"
        url = (
            f"{self.base_url}?sort=bestMatch&order=asc&isRefinedSearch=true"
            f"&verticalId={care_type}&radius=30"
            f"&geoRegionId={geo_region_id}&geoRegionSearch={geo_region_search}"
            f"&max={self.results_per_page}&offset=0&id=pagination"
        )
        return url

    def extract_top20_profile_ids(self, html: str) -> List[str]:
        """
        Extracts the top 20 caregiver profile IDs from the HTML content.

        The regex used looks for:
            class="entityId" name="entityId" value="9138062"

        Args:
            html (str): HTML content from the search results page.
        
        Returns:
            List[str]: A list of profile ID strings (up to 20).
        """
        pattern = re.compile(r'class="entityId" name="entityId" value="(\d+)"')
        ids = pattern.findall(html)
        # Return only the first 20 results.
        return ids[:20]

    def scrape_top20(self, care_type: str) -> Tuple[List[str], dict]:
        """
        Scrapes the top 20 caregiver profiles for a given care type and returns their profile IDs along with metadata.

        Args:
            care_type (str): The care type (vertical ID) to scrape.
        
        Returns:
            Tuple[List[str], dict]: A tuple containing:
                - A list of profile ID strings (up to 20).
                - A metadata dictionary.
        """
        url = self.build_search_url(care_type)
        self.logger.info(f"Fetching top 20 caregivers for care type '{care_type}' from URL: {url}")
        metadata = {
            "care_type": care_type,
            "scrape_time": datetime.utcnow().isoformat() + "Z",
            "url": url,
            "status_code": None,
            "error": None
        }
        try:
            response = self.session.get(url)
            metadata["status_code"] = response.status_code
            response.raise_for_status()
            html = response.text
            profile_ids = self.extract_top20_profile_ids(html)
            self.logger.info(f"Extracted {len(profile_ids)} profile IDs for care type '{care_type}'.")
        except Exception as e:
            self.logger.error(f"Error fetching top 20 caregivers for care type '{care_type}': {e}")
            profile_ids = []
            metadata["error"] = str(e)
        return profile_ids, metadata
    
    def save_monitoring_result(self, care_type: str, profile_ids: List[str], metadata: dict):
        """
        Saves the top 20 profile IDs and the corresponding metadata to JSON files.

        The filenames include a UTC timestamp to distinguish different monitoring cycles.
        The files are saved under:
            data/monitoring/France/<postal_code>/monitor_of_<care_type>/

        Args:
            care_type (str): The care type (vertical ID) monitored.
            profile_ids (List[str]): List of profile ID strings.
            metadata (dict): Metadata for the scrape cycle.
        """
        # Define output folder for this care type.
        output_folder = os.path.join(self.base_output_folder, f"monitor_of_{care_type}")
        os.makedirs(output_folder, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        result_file = os.path.join(output_folder, f"top20_{timestamp}.json")
        metadata_file = os.path.join(output_folder, f"metadata_top20_{timestamp}.json")
        with open(result_file, "w", encoding="utf-8") as rf:
            json.dump(profile_ids, rf, ensure_ascii=False, indent=4)
        with open(metadata_file, "w", encoding="utf-8") as mf:
            json.dump(metadata, mf, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved top 20 results and metadata for care type '{care_type}' at timestamp {timestamp}.")

    def run_monitor(self):
        """
        Runs the monitoring pipeline:
          - For the specified total_days, at every interval_hours, iterates over each care type.
          - For each care type, scrapes the top 20 caregiver profile IDs and saves the results along with metadata.
          - Sleeps for the specified interval between each full cycle (covering all care types).
        """
        self.logger.info(f"Starting monitoring for {self.total_days} days, scraping every {self.interval_hours} hours for care types: {self.care_types}")
        start_time = time.time()
        total_duration = self.total_days * 24 * 3600  # total duration in seconds
        iteration = 0
        
        while (time.time() - start_time) < total_duration:
            iteration += 1
            self.logger.info(f"Iteration {iteration}: Starting scrape cycle for all care types.")
            for care_type in self.care_types:
                profile_ids, metadata = self.scrape_top20(care_type)
                self.save_monitoring_result(care_type, profile_ids, metadata)
            self.logger.info(f"Iteration {iteration}: Completed scrape cycle. Sleeping for {self.interval_hours} hours.")
            time.sleep(self.interval_hours * 3600)
        self.logger.info("Monitoring period complete.")

if __name__ == "__main__":
    # Example usage: Monitor top 20 caregivers for both "childCare" and "seniorCare" every 2 hours for 1 day.
    monitor = TopCaregiversMonitor(
         postal_code="75001",
         care_types=["childCare", "seniorCare"],
         interval_hours=2,
         total_days=1,
         auth_token=None  # Replace with token string if authentication is needed.
    )
    monitor.run_monitor()
