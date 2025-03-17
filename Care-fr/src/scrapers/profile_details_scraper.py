"""
File: src/scrapers/profile_details_scraper.py
Author: Ayoub FRIHAOUI
Date: 03/14/2025

Description:
-------------
This module defines the ProfileDetailsScraper class, which is responsible for scraping detailed 
profile data for each caregiver profile ID found in a previously scraped JSON file.
The input file is located at:
    data/raw_data/France/<postal_code>/search_of_<care_type>/profileIDs.json
The scraper automatically extracts the care_type from the input file's directory name.
For each profile ID, the scraper sends a GET request to:
    https://www.care.com/fr-fr/api/v1/profiles-details-view/<profileID>
The detailed profile JSON is saved as:
    data/raw_data/France/<postal_code>/Profiles_of_<care_type>/<profileID>.json
Additionally, a metadata file for each profile is created:
    data/raw_data/France/<postal_code>/Profiles_of_<care_type>/metada_<profileID>.json
The metadata includes the scrape timestamp, HTTP status code, URL, and any error messages.

Usage:
------
Run the module as a script:
    python -m src.scrapers.profile_details_scraper

Dependencies:
-------------
- os, json, time, sys, requests, datetime, pathlib, logging, re, typing
- Logger utility from src/utils/logger.py for logging.
"""

import os
import json
import time
import sys
import requests
import random
from datetime import datetime
from pathlib import Path
from typing import Tuple  # Import Tuple for type annotations

# Append parent directory to sys.path so that modules in utils can be imported
sys.path.append('..')
from utils.logger import get_logger

class ProfileDetailsScraper:
    def __init__(self, input_file: str, auth_token: str = None):
        """
        Initialize the ProfileDetailsScraper with the input JSON file containing profile IDs.
        
        Args:
            input_file (str): Path to the input JSON file (e.g., 
                              "data/raw_data/France/75001/search_of_childCare/profileIDs.json").
            auth_token (str): Optional authentication token or cookie string.
        """
        self.input_file = input_file
        self.auth_token = auth_token
        self.logger = get_logger(self.__class__.__name__)
        
        # Load input JSON data containing profile IDs and metadata
        with open(self.input_file, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        self.profile_ids = self.data.get("profileIDs", [])
        
        # Extract postal_code and care_type from the input file path.
        # Expected input file path:
        # data/raw_data/France/<postal_code>/search_of_<care_type>/profileIDs.json
        input_path = Path(self.input_file)
        self.postal_code = input_path.parent.parent.name  # e.g., "75001"
        search_folder_name = input_path.parent.name         # e.g., "search_of_childCare"
        if search_folder_name.startswith("search_of_"):
            # Extract care_type from the folder name (e.g., "childCare")
            self.care_type = search_folder_name.replace("search_of_", "")
        else:
            self.care_type = "unknown"
            self.logger.warning("Could not determine care_type from folder name; defaulting to 'unknown'.")
        
        # Set the base URL for profile details API.
        self.base_details_url = "https://www.care.com/fr-fr/api/v1/profiles-details-view/"
        
        # Define output folder automatically as:
        # data/raw_data/France/<postal_code>/Profiles_of_<care_type>
        self.output_folder = os.path.join(".." ,"data", "raw_data", "France", self.postal_code, f"Profiles_of_{self.care_type}")
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Initialize a requests session; update headers if auth token is provided.
        self.session = requests.Session()
        if self.auth_token:
            self.session.headers.update({"Cookie": self.auth_token})
    
    def build_profile_url(self, profile_id: str) -> str:
        """
        Constructs the URL for retrieving detailed profile data for a given profile ID.
        
        Args:
            profile_id (str): The caregiver profile ID.
        
        Returns:
            str: The constructed URL.
        """
        return f"{self.base_details_url}{profile_id}"
    
    def fetch_profile_details(self, profile_id: str) -> Tuple[dict, dict]:
        """
        Fetches detailed profile data for a given profile ID.
        
        Args:
            profile_id (str): The caregiver profile ID.
        
        Returns:
            Tuple[dict, dict]: A tuple containing:
                - profile_data (dict): The JSON response from the API (empty dict if error).
                - metadata (dict): Scrape metadata including timestamp, status code, URL, and error message.
        """
        url = self.build_profile_url(profile_id)
        self.logger.info(f"Fetching profile details for ID {profile_id} from URL: {url}")
        metadata = {
            "profile_id": profile_id,
            "scrape_time": datetime.utcnow().isoformat() + "Z",
            "url": url,
            "status_code": None,
            "error": None
        }
        try:
            response = self.session.get(url)
            metadata["status_code"] = response.status_code
            response.raise_for_status()
            profile_data = response.json()
            self.logger.info(f"Successfully fetched profile details for ID {profile_id}")
        except Exception as e:
            self.logger.error(f"Error fetching details for profile ID {profile_id}: {e}")
            profile_data = {}
            metadata["error"] = str(e)
        return profile_data, metadata
    
    def save_profile_data(self, profile_id: str, profile_data: dict, metadata: dict):
        """
        Saves the profile details and associated metadata to JSON files.
        
        Output files:
            - <profile_id>.json for profile details.
            - metada_<profile_id>.json for metadata.
        
        Args:
            profile_id (str): The caregiver profile ID.
            profile_data (dict): The detailed profile JSON data.
            metadata (dict): Metadata associated with this profile scrape.
        """
        profile_file = os.path.join(self.output_folder, f"{profile_id}.json")
        metadata_file = os.path.join(self.output_folder, f"metada_{profile_id}.json")
        with open(profile_file, "w", encoding="utf-8") as pf:
            json.dump(profile_data, pf, ensure_ascii=False, indent=4)
        with open(metadata_file, "w", encoding="utf-8") as mf:
            json.dump(metadata, mf, ensure_ascii=False, indent=4)
        self.logger.info(f"Saved profile data and metadata for ID {profile_id} to {self.output_folder}")
    
    def scrape_profiles(self):
        """
        Iterates over all profile IDs from the input file, fetches detailed profile data,
        and saves both the data and metadata for each profile.
        """
        self.logger.info(f"Starting profile details scraping for {len(self.profile_ids)} profile IDs.")
        for profile_id in self.profile_ids:
            try:
                profile_data, metadata = self.fetch_profile_details(profile_id)
                self.save_profile_data(profile_id, profile_data, metadata)
                # Introduce a delay between requests to reduce load and avoid rate limiting.
                time.sleep(round(random.uniform(0.8, 1.2), 4))
            except Exception as e:
                break
                
        self.logger.info("Completed scraping all profile details.")

if __name__ == "__main__":
    # Define the input file path (adjust as needed).
    input_file_path = os.path.join("data", "raw_data", "France", "75001", "search_of_childCare", "profileIDs.json")
    scraper = ProfileDetailsScraper(input_file=input_file_path, auth_token=None)
    scraper.scrape_profiles()