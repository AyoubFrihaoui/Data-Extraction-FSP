"""
File: src/monitors/top_caregivers_monitor.py
Author: @AyoubFrihaoui
Date: 03/17/2025

Description:
-------------
This module defines the TopCaregiversMonitor class, which is responsible for monitoring the top 20
caregiver profiles displayed on the care.fr platform. The pipeline performs the following tasks:
  
  1. Scrapes the top 20 caregiver profile IDs from the search page every 2 hours.
  2. Captures metadata for each scrape (including timestamp, HTTP status code, URL, and any error).
  3. Saves the top 20 profile IDs and associated metadata into separate JSON files.
  
The output files are stored under:
    data/monitoring/France/<postal_code>/monitor_of_<care_type>/
where <care_type> is automatically extracted from the configuration (e.g., "childCare").

This pipeline is intended for monitoring changes over time to study the recommendation algorithm and
to observe potential inequalities in the platform's ranking. The monitor will run for a specified
number of days.

Usage:
------
Run this module as a script:
    python -m src.monitors.top_caregivers_monitor

Dependencies:
-------------
- Python standard libraries: re, json, os, time, datetime, sys
- requests: For HTTP requests.
- Logger utility from src/utils/logger.py for logging purposes.
- The output folder is automatically created if it does not exist.
"""

import re
import json
import os
import time
from datetime import datetime
import requests
import sys
from typing import Tuple

# Append parent directory to sys.path so that modules in utils can be imported.
sys.path.append('..')
from utils.logger import get_logger

class TopCaregiversMonitor:
    def __init__(self, postal_code: str, care_type: str, interval_hours: float, total_days: float, auth_token: str = None):
        """
        Initialize the monitor with search parameters and scheduling details.
        
        Args:
            postal_code (str): The postal code to search in (e.g., "75001").
            care_type (str): The care type (vertical ID) to search (e.g., "childCare", "auPair").
            interval_hours (float): The interval between scrapes (in hours).
            total_days (float): Total monitoring duration in days.
            auth_token (str): Optional authentication token or cookie string.
        """
        self.postal_code = postal_code
        self.care_type = care_type
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
        
        # Define the output folder for monitoring results:
        # data/monitoring/France/<postal_code>/monitor_of_<care_type>
        self.output_folder = os.path.join("data", "monitoring", "France", self.postal_code, f"monitor_of_{self.care_type}")
        os.makedirs(self.output_folder, exist_ok=True)
    
    def build_search_url(self) -> str:
        """
        Constructs the search URL to fetch the top 20 caregiver profiles.
        
        Returns:
            str: The constructed URL with max=20 and offset=0.
        """
        geo_region_id = f"POSTCODE-FR-{self.postal_code}"
        geo_region_search = f"{self.postal_code}"
        # We fix offset=0 and max=20 to get the top 20 caregivers.
        url = (
            f"{self.base_url}?sort=bestMatch&order=asc&isRefinedSearch=true"
            f"&verticalId={self.care_type}&radius=30"
            f"&geoRegionId={geo_region_id}&geoRegionSearch={geo_region_search}"
            f"&max={self.results_per_page}&offset=0&id=pagination"
        )
        return url
    
    def extract_top20_profile_ids(self, html: str) -> list:
        """
        Extracts the top 20 caregiver profile IDs from the HTML content.
        
        The regex used looks for:
            class="entityId" name="entityId" value="9138062"
        
        Args:
            html (str): HTML content from the search results page.
        
        Returns:
            list: A list of profile ID strings (up to 20).
        """
        pattern = re.compile(r'class="entityId" name="entityId" value="(\d+)"')
        ids = pattern.findall(html)
        # Return only the first 20 results.
        return ids[:20]
    
    def scrape_top20(self) -> Tuple[list, dict]:
        """
        Scrapes the top 20 caregiver profiles and returns their profile IDs along with metadata.

        Returns:
            Tuple[list, dict]: A tuple containing:
                - A list of profile ID strings (up to 20).
                - A metadata dictionary.
        """
        url = self.build_search_url()
        self.logger.info(f"Fetching top 20 caregivers from URL: {url}")
        metadata = {
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
            self.logger.info(f"Extracted {len(profile_ids)} profile IDs.")
        except Exception as e:
            self.logger.error(f"Error fetching top 20 caregivers: {e}")
            profile_ids = []
            metadata["error"] = str(e)
        return profile_ids, metadata
    
    def save_monitoring_result(self, profile_ids: list, metadata: dict):
        """
        Saves the top 20 profile IDs and the corresponding metadata to JSON files.
        
        The filenames include a UTC timestamp to distinguish different monitoring cycles.
        
        Args:
            profile_ids (list): List of profile ID strings.
            metadata (dict): Metadata for the scrape cycle.
        """
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        result_file = os.path.join(self.output_folder, f"top20_{timestamp}.json")
        metadata_file = os.path.join(self.output_folder, f"metadata_top20_{timestamp}.json")
        with open(result_file, "w", encoding="utf-8") as rf:
            json.dump(profile_ids, rf, ensure_ascii=False, indent=4)
        with open(metadata_file, "w", encoding="utf-8") as mf:
            json.dump(metadata, mf, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved top 20 results and metadata for timestamp {timestamp}.")
    
    def run_monitor(self):
        """
        Runs the monitoring pipeline:
          - Scrapes the top 20 caregiver profiles every `interval_hours` hours.
          - Continues monitoring for `total_days` days.
          - Saves each scrape cycle's results and metadata.
        """
        self.logger.info(f"Starting monitoring for {self.total_days} days, scraping every {self.interval_hours} hours.")
        start_time = time.time()
        total_duration = self.total_days * 24 * 3600  # total duration in seconds
        iteration = 0
        while (time.time() - start_time) < total_duration:
            iteration += 1
            self.logger.info(f"Iteration {iteration}: Starting scrape cycle.")
            profile_ids, metadata = self.scrape_top20()
            self.save_monitoring_result(profile_ids, metadata)
            self.logger.info(f"Iteration {iteration}: Completed scrape cycle. Sleeping for {self.interval_hours} hours.")
            time.sleep(self.interval_hours * 3600)
        self.logger.info("Monitoring period complete.")

if __name__ == "__main__":
    # Example usage: monitor top 20 caregivers every 2 hours for 1 day.
    monitor = TopCaregiversMonitor(
         postal_code="75001",
         care_type="childCare",
         interval_hours=2,
         total_days=1,
         auth_token=None  # Replace with token string if authentication is needed.
    )
    monitor.run_monitor()
