"""
MaltProfileHtmlScraper: DataOps-Oriented HTML Profile Scraper for Malt.fr (v1.0.0)

PURPOSE:
  1) Traverses a directory structure previously populated by a Malt.fr JSON API scraper
     (e.g., MaltScraper v1.2.2).
  2) Reads `page_*.json` files from `range_<min>_<max>` subdirectories.
  3) Extracts unique freelancer profile IDs from these JSON files.
  4) Fetches the full HTML content for each unique profile ID from `https://www.malt.fr/profile/<id>`.
  5) Saves the fetched HTML content into a new, dedicated directory structure:
     `Malt-fr/raw_data/malt_fr/<query_slug>/<location_slug>/profiles/<profile_id>.html`.
  6) Operates in parallel using ThreadPoolExecutor for fetching HTML pages.
  7) Implements DataOps best practices including comprehensive logging and metadata generation
     for the HTML scraping process and the output directories.

ASSUMPTIONS:
  - This script assumes that the JSON data from the search API has already been scraped
    and is available in the expected directory structure:
    `Malt-fr/raw_data/malt_fr/<query_slug>/<location_slug>/range_<min>_<max>/page_N.json`.
  - `config.py` exists and contains `MALT_COOKIE` and `MALT_XSRF_TOKEN` which might be needed
    for accessing profile pages (even if public, a session can be beneficial).

INPUT DIRECTORY STRUCTURE (Example - created by previous scraper):
  Malt-fr/raw_data/
    malt_fr/
      <query_slug>/
        <location_slug>/
          range_<min_price>_<max_price>/
            page_1.json
            page_2.json
            ...

OUTPUT DIRECTORY STRUCTURE (Created by this script):
  Malt-fr/raw_data/
    malt_fr/
      <query_slug>/
        <location_slug>/
          profiles/                             # New directory for HTML profiles
            metadata_profiles.json              # Metadata for this HTML scraping run for this query/location
            <profile_id_1>.html
            <profile_id_2>.html
            ...

LOGGING:
  - A log file `scrape_html_profiles_<query_slug>_<location_slug>.log` is created in the
    `<location_slug>` directory (alongside the `profiles` directory).
  - Logs activities like ID discovery, HTML fetching status, errors, and summary.

METADATA:
  - `metadata_profiles.json`: Created inside each `profiles` directory. Contains details
    about the HTML scraping run for that specific query/location combination, including
    total IDs found, HTMLs fetched, errors, duration, etc.
  - (Optional future enhancement: individual metadata per HTML file if granular detail is needed).

DISCLAIMER:
  - This code is for educational purposes. Respect Malt.fr's Terms of Service.
  - Adjust `num_workers` and sleep timers to be polite to the server.

Author: Your Name / @AyoubFrihaoui
Version: 1.0.0
"""

import os
import sys
import time
import json
from typing import Dict, Optional
import uuid
import random
import logging
import platform
import requests
import re
import threading  # For potential future use with stop_event, though less critical here if processing all found IDs
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

# --- Configuration Import ---
try:
    from config import MALT_COOKIE, MALT_XSRF_TOKEN
except ImportError:
    print(
        "ERROR: config.py not found. Please create it with MALT_COOKIE and MALT_XSRF_TOKEN."
    )
    MALT_COOKIE = "YOUR_MALT_COOKIE_PLACEHOLDER"
    MALT_XSRF_TOKEN = "YOUR_MALT_XSRF_TOKEN_PLACEHOLDER"


class MaltProfileHtmlScraper:
    """
    Scrapes individual Malt.fr profile HTML pages based on IDs extracted from
    previously saved JSON search result files.
    """

    BASE_PROFILE_URL = "https://www.malt.fr/profile/"
    # Headers for requesting HTML content
    HTML_REQUEST_HEADERS = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",
        "cache-control": "no-cache",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "priority": "u=0, i",  # Can be u=0 for top-level navigation
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",  # Typically 'none' for direct URL access
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    }

    def __init__(
        self,
        base_input_dir: str = os.path.join("Malt-fr", "raw_data", "malt_fr"),
        num_workers: int = 8,
        cookie: str = MALT_COOKIE,
        xsrf_token: str = MALT_XSRF_TOKEN,
    ):  # Keep XSRF in case some profile interactions might need it.
        """
        Initializes the HTML Profile Scraper.

        :param base_input_dir: The root directory containing the query/location subdirectories
                               with previously scraped JSON search results.
                               Example: "Malt-fr/raw_data/malt_fr"
        :param num_workers: Number of parallel threads for fetching HTML profile pages.
        :param cookie: Malt.fr cookie string from config.py.
        :param xsrf_token: Malt.fr XSRF token string from config.py.
        """
        self.base_input_dir = base_input_dir
        if not os.path.isdir(self.base_input_dir):
            print(
                f"ERROR: Base input directory '{self.base_input_dir}' not found. Please ensure JSON data exists."
            )
            sys.exit(1)

        self.num_workers = num_workers
        self.run_id = str(uuid.uuid4())  # Unique ID for this HTML scraping run

        self.session = requests.Session()
        # Set up base session headers, including Cookie. Specific HTML headers will be merged per request.
        base_session_headers = {
            #"Cookie": cookie,
            "Cookie": "malt-visitorId=890b144a-3598-4bd4-8229-81bc983507e8; OptanonAlertBoxClosed=2025-04-17T12:12:06.904Z; _gcl_au=1.1.139234146.1744891961; _fbp=fb.1.1744893115162.530334627927976086; i18n=fr-FR; remember-me=a2dZUVJ2SXlES2lCZGJvUnZTanZTUSUzRCUzRDp1SFE2RllzZFB6OXlyQzZuUjdhbVp3JTNEJTNE; XSRF-TOKEN=812e6b4d-46ef-4136-89f6-f0bb8aac62db; hopsi=67e1607b991b4d70bad837d1; SESSION=MDM5NjJmZDYtNmVmYi00ZmI2LWJiODctZmIzMTIyZGRkZDll; JSESSIONID=90A5EEA3238B6B62A8CAA4F82891D317; __cfruid=de9e12aafd4c0dc6e0be65cd6370e4dbc9d0a5d6-1747665367; __cf_bm=upEuVX_khLA.K.3y21GffBfTBgJBZoyAF_xnrWxxjTA-1747665367-1.0.1.1-5jN.oB92fPEg._rmBQxqp5fsHGDN7bI0Dezvuc6TxRgUqz5xafxpj0CpUYicmUHg9wdP2Jx5KePBzIxRnC4wJkHLnYxJFbi.yREKHjmFN8ny8dKZHMlDAAAHrw1pC0bX; cf_clearance=bkJOoh2Uv11fhSTOmyL2ct3Y0AOjiBbYYCpm1U9PIMU-1747665738-1.2.1.1-YETeTMSvOtxRWBGiZ8l1hGHvhHdOBJedXNp8m4wB.uU68vf026EwDr9jAdmRJ7Cyz0qabogoe5zGEg3FRwTYP5G0YwF1S09jmkG2N4GVjPWC9etIzPVbXzXocgY.OJPjtWfOe5eaPDvnW8gvGDPustqmjZIx278.qJ8KhbNo6qiMGrR3PHVDDpIWbbZ2EiKZvwacZR_sXrVfKflbxLwW7kpPTGc_eX4nft7zJnF70t.z4hKRwHX7Ih2Jb_QxBVwkuyS7cZFqkd1mq9H62_VXBfcL5N3EB0Lg5QoBL4UnPxAasV8VUdmMor1JtSownk1hBI6WWjy2P12M5znczHZv9Ta4qP7g1IMHYUKCeeHnk_w; OptanonConsent=isGpcEnabled=0&datestamp=Mon+May+19+2025+16%3A45%3A31+GMT%2B0200+(Central+European+Summer+Time)&version=202211.2.0&isIABGlobal=false&hosts=&consentId=c99c6217-5697-4401-8fbd-61a443497739&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0005%3A1&geolocation=%3B&AwaitingReconsent=false",
            #"x-xsrf-token": xsrf_token,  # May or may not be needed for profile GETs but good to have
            "user-agent": self.HTML_REQUEST_HEADERS[
                "user-agent"
            ],  # Ensure consistent UA
        }
        self.session.headers.update(base_session_headers)

        # Logger setup (will be configured per query/location processing)
        self.logger = None

        if cookie.startswith("YOUR_") or xsrf_token.startswith("YOUR_"):
            print(
                "WARNING: Using placeholder Cookie/XSRF-Token from config.py. Profile fetching might fail or be limited."
            )

    def _setup_logger_for_context(
        self, query_slug: str, location_slug: str, current_location_path: str
    ):
        """
        Configures a logger specific to the current query and location being processed.
        Log file will be placed in the location_slug directory.

        :param query_slug: Slug of the current query term.
        :param location_slug: Slug of the current location.
        :param current_location_path: Path to the current location_slug directory.
        """
        log_file_name = f"scrape_html_profiles_{query_slug}_{location_slug}.log"
        log_file_path = os.path.join(current_location_path, log_file_name)

        # Create a unique logger name to avoid conflicts if processing multiple contexts
        logger_name = (
            f"MaltProfileHtmlScraper_{self.run_id}_{query_slug}_{location_slug}"
        )
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        # Prevent adding multiple handlers if called multiple times for the same logger
        if not self.logger.handlers:
            # File Handler
            fh = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            # Console Handler
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.INFO)
            # Formatter
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s [%(module)s:%(lineno)d] - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            # Add Handlers
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)
        self.logger.info(
            f"HTML Profile Scraper logging initialized for {query_slug}/{location_slug}. Log file: {log_file_path}"
        )

    def _update_run_metadata(
        self, metadata_file_path: str, run_status: str, summary_data: dict
    ):
        """
        Creates or updates a metadata JSON file for the HTML scraping run within a 'profiles' directory.

        :param metadata_file_path: Full path to the metadata_profiles.json file.
        :param run_status: The status of this HTML scraping run ("processing", "success", "error", "completed_with_errors").
        :param summary_data: A dictionary containing summary statistics for the run.
        """
        metadata = {
            "deception": "This file contains metadata about the scraping run that fetched individual Malt profile HTML pages for this query/location.",
            "html_scrape_run_id": self.run_id,
            "run_start_time_utc": summary_data.get(
                "run_start_time_utc", datetime.now(timezone.utc).isoformat()
            ),
            "run_status": run_status,
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        metadata.update(summary_data)  # Merge summary data

        try:
            with open(metadata_file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            if self.logger:
                self.logger.info(
                    f"Updated run metadata at {metadata_file_path} with status: {run_status}"
                )
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Failed to write run metadata to {metadata_file_path}: {e}"
                )

    def discover_profile_ids(self, current_location_path: str) -> set:
        """
        Scans all `range_*/page_*.json` files within the given location path
        to extract unique profile IDs.

        :param current_location_path: Path to the directory for a specific query/location
                                     (e.g., "Malt-fr/raw_data/malt_fr/developpeur_python/paris").
        :return: A set of unique profile IDs found.
        """
        unique_profile_ids = set()
        if not self.logger:
            print(
                "ERROR: Logger not initialized in discover_profile_ids. Call _setup_logger_for_context first."
            )
            return unique_profile_ids

        self.logger.info(f"Discovering profile IDs within: {current_location_path}")
        range_dirs_found = 0
        json_files_processed = 0

        for item_name in os.listdir(current_location_path):
            item_path = os.path.join(current_location_path, item_name)
            if os.path.isdir(item_path) and item_name.startswith(
                "range_"
            ):  # Check if it's a range directory
                range_dirs_found += 1
                self.logger.debug(f"  Scanning range directory: {item_name}")
                for page_file_name in os.listdir(item_path):
                    if page_file_name.startswith("page_") and page_file_name.endswith(
                        ".json"
                    ):
                        page_file_path = os.path.join(item_path, page_file_name)
                        try:
                            with open(page_file_path, "r", encoding="utf-8") as f:
                                data = json.load(f)
                            profiles = data.get("profiles", [])
                            ids_found_in_file = 0
                            for profile in profiles:
                                profile_id = profile.get("id")
                                if profile_id:
                                    unique_profile_ids.add(profile_id)
                                    ids_found_in_file += 1
                            if ids_found_in_file > 0:
                                self.logger.debug(
                                    f"    Found {ids_found_in_file} IDs in {page_file_name}"
                                )
                            json_files_processed += 1
                        except json.JSONDecodeError:
                            self.logger.error(
                                f"    Error decoding JSON from {page_file_path}. Skipping."
                            )
                        except Exception as e:
                            self.logger.error(
                                f"    Error reading {page_file_path}: {e}. Skipping."
                            )
        self.logger.info(
            f"Discovery complete: Found {len(unique_profile_ids)} unique profile IDs across {json_files_processed} JSON files in {range_dirs_found} range directories."
        )
        return unique_profile_ids

    def _fetch_single_profile_html_worker(
        self, profile_id: str, html_output_dir: str, i: int
    ) -> tuple[str, bool, Optional[str]]:
        """
        Worker function to fetch HTML for a single profile ID.

        :param profile_id: The ID of the profile.
        :param html_output_dir: Directory to save the HTML file.
        :return: Tuple of (profile_id, success_status, error_message_or_None).
        """
        profile_url = f"{self.BASE_PROFILE_URL}{profile_id}"
        html_file_path = os.path.join(html_output_dir, f"{profile_id}.html")

        # Prepare headers for HTML request, inheriting from session and adding HTML specifics
        request_headers = (
            self.session.headers.copy()
        )  # Get session headers (Cookie, base UA, XSRF)
        request_headers.update(
            self.HTML_REQUEST_HEADERS
        )  # Merge/override with HTML specific headers
        request_headers["referer"] = (
            f"{self.BASE_PROFILE_URL}"  # Set a plausible referer
        )

        try:
            if self.logger:
                self.logger.debug(
                    f"Fetching HTML for profile ID {profile_id} from {profile_url}"
                )
            # response = self.session.get(
            #     profile_url, headers=request_headers, timeout=40
            # )  # Use the main session
            # response.raise_for_status()


            url = profile_url

            payload = {}
            headers = {
              'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
              'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
              'priority': 'u=0, i',
              'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"Windows"',
              'sec-fetch-dest': 'document',
              'sec-fetch-mode': 'navigate',
              'sec-fetch-site': 'none',
              'sec-fetch-user': '?1',
              'upgrade-insecure-requests': '1',
              'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
              'Cookie': MALT_COOKIE
            }

            response = requests.request("GET", url, headers=headers, data=payload)







            # if i == 1:
            #     print(response.text)

            with open(html_file_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            if self.logger:
                self.logger.debug(
                    f"  Successfully saved HTML for profile ID {profile_id}"
                )

            # Short, random sleep after each successful HTML fetch by a worker
            time.sleep(round(random.uniform(1.9, 3.1), 3))  # Adjusted sleep
            return profile_id, True, None

        except requests.exceptions.Timeout:
            msg = f"Timeout fetching HTML for profile ID {profile_id}"
            if self.logger:
                self.logger.warning(msg)
            return profile_id, False, msg
        except requests.exceptions.HTTPError as http_err:
            msg = f"HTTP error {http_err.response.status_code} fetching HTML for profile {profile_id}: {http_err}"
            if self.logger:
                self.logger.warning(
                    msg
                )  # Log as warning, as some profiles might be private/deleted
            return profile_id, False, msg
        except Exception as e:
            msg = f"Generic error fetching HTML for profile ID {profile_id}: {e}"
            if self.logger:
                self.logger.error(
                    msg, exc_info=False
                )  # Keep log concise for many errors
            return profile_id, False, msg

    def process_query_location(self, query_slug: str, location_slug: str):
        """
        Processes a single query/location: discovers IDs, fetches HTMLs, saves, and creates metadata.
        """
        current_location_path = os.path.join(
            self.base_input_dir, query_slug, location_slug
        )
        if not os.path.isdir(current_location_path):
            print(f"Skipping: Path not found {current_location_path}")
            return

        # Setup logger for this specific query/location context
        self._setup_logger_for_context(query_slug, location_slug, current_location_path)
        self.logger.info(
            f"--- Starting HTML Profile Scraping for: {query_slug} / {location_slug} ---"
        )
        self.logger.info(f"Run ID for this HTML scrape: {self.run_id}")

        # --- Define Output Directory and Metadata File for HTMLs ---
        # html_output_dir = os.path.join(
        #     current_location_path, "profiles"
        # )  # New 'profiles' subdir
        html_output_dir = "H:/Data Extraction FSP/Malt-fr/raw_data/malt_fr/developpeur/en_télétravail/profiles"
        os.makedirs(html_output_dir, exist_ok=True)
        run_metadata_file = os.path.join(html_output_dir, "metadata_profiles.json")

        # --- Initial Run Metadata ---
        run_start_time = time.time()
        summary_data = {
            "query_slug": query_slug,
            "location_slug": location_slug,
            "run_start_time_utc": datetime.now(timezone.utc).isoformat(),
            "status_details": "ID Discovery",
        }
        self._update_run_metadata(
            run_metadata_file, "processing_id_discovery", summary_data
        )

        # --- 1. Discover all unique profile IDs ---
        profile_ids_to_fetch = self.discover_profile_ids(current_location_path)

        if not profile_ids_to_fetch:
            self.logger.info(
                "No profile IDs found to fetch for this context. Skipping HTML scraping."
            )
            summary_data.update(
                {
                    "total_unique_ids_found": 0,
                    "html_profiles_fetched_successfully": 0,
                    "html_profiles_failed": 0,
                    "run_end_time_utc": datetime.now(timezone.utc).isoformat(),
                    "duration_seconds": round(time.time() - run_start_time, 2),
                    "status_details": "No IDs found",
                }
            )
            self._update_run_metadata(run_metadata_file, "success_no_ids", summary_data)
            self.logger.info("--- HTML Profile Scraping Finished (No IDs) ---")
            return

        summary_data.update(
            {
                "total_unique_ids_found": len(profile_ids_to_fetch),
                "status_details": "HTML Fetching",
            }
        )
        self._update_run_metadata(
            run_metadata_file, "processing_html_fetching", summary_data
        )

        # --- 2. Fetch HTML for each profile ID in parallel ---
        self.logger.info(
            f"Starting HTML fetch for {len(profile_ids_to_fetch)} unique profile IDs using {self.num_workers} workers."
        )
        html_fetched_count = 0
        html_failed_count = 0
        futures_map: Dict[Future, str] = {}  # Future -> profile_id

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            i = 0
            for profile_id in profile_ids_to_fetch:
                future = executor.submit(
                    self._fetch_single_profile_html_worker, profile_id, html_output_dir, i
                )
                futures_map[future] = profile_id
                i += 1

            for i, future in enumerate(as_completed(futures_map)):
                profile_id = futures_map[future]
                try:
                    _, success, error_msg = future.result()
                    if success:
                        html_fetched_count += 1
                    else:
                        html_failed_count += 1
                        if error_msg:  # Log specific error for failed profile
                            self.logger.debug(
                                f"Failed HTML fetch for {profile_id}: {error_msg}"
                            )
                    if (i + 1) % 50 == 0:  # Log progress periodically
                        self.logger.info(
                            f"  Progress: Processed {i+1}/{len(profile_ids_to_fetch)} profile IDs (Fetched: {html_fetched_count}, Failed: {html_failed_count})"
                        )

                except Exception as e_future:
                    html_failed_count += 1
                    self.logger.error(
                        f"Error processing result for profile ID {profile_id}: {e_future}",
                        exc_info=True,
                    )

        run_end_time = time.time()
        duration = round(run_end_time - run_start_time, 2)

        self.logger.info(
            f"HTML Fetching Complete. Successfully fetched: {html_fetched_count}, Failed: {html_failed_count}."
        )
        self.logger.info(
            f"Total duration for {query_slug}/{location_slug}: {duration} seconds."
        )

        # --- 3. Final Metadata Update ---
        final_status = "completed_with_errors" if html_failed_count > 0 else "success"
        if (
            html_fetched_count == 0
            and html_failed_count == len(profile_ids_to_fetch)
            and len(profile_ids_to_fetch) > 0
        ):
            final_status = "error_all_failed"

        summary_data.update(
            {
                "html_profiles_fetched_successfully": html_fetched_count,
                "html_profiles_failed": html_failed_count,
                "run_end_time_utc": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": duration,
                "status_details": "Completed",
            }
        )
        self._update_run_metadata(run_metadata_file, final_status, summary_data)
        self.logger.info(
            f"--- HTML Profile Scraping Finished for: {query_slug} / {location_slug} ---"
        )

    def run_overall_scraper(self):
        """
        Main orchestrator. Iterates through all query_slug and location_slug directories
        found in the `base_input_dir` and processes each one.
        """
        print(f"Starting overall HTML Profile Scraping Process. Run ID: {self.run_id}")
        print(f"Scanning base input directory: {self.base_input_dir}")

        if not os.path.exists(self.base_input_dir):
            print(
                f"CRITICAL: Base input directory '{self.base_input_dir}' does not exist. Cannot proceed."
            )
            return

        query_slug_dirs = [
            d
            for d in os.listdir(self.base_input_dir)
            if os.path.isdir(os.path.join(self.base_input_dir, d))
        ]

        if not query_slug_dirs:
            print(
                f"No query directories found in '{self.base_input_dir}'. Nothing to process."
            )
            return

        print(
            f"Found {len(query_slug_dirs)} query directories to process: {query_slug_dirs}"
        )

        for query_slug in query_slug_dirs:
            query_path = os.path.join(self.base_input_dir, query_slug)
            location_slug_dirs = [
                d
                for d in os.listdir(query_path)
                if os.path.isdir(os.path.join(query_path, d))
            ]

            if not location_slug_dirs:
                print(
                    f"No location directories found in '{query_path}' for query '{query_slug}'."
                )
                continue

            print(
                f"  Processing query '{query_slug}': Found {len(location_slug_dirs)} location(s)."
            )
            for location_slug in location_slug_dirs:
                # Each query/location gets its own run_id for its metadata_profiles.json,
                # but they all share the overall script's self.run_id for top-level tracking if needed.
                # For simplicity here, we'll let each process_query_location handle its own metadata based on the shared self.run_id.
                self.process_query_location(query_slug, location_slug)
                # Optional: add a small delay between processing different location contexts
                time.sleep(random.uniform(5, 7))

        print(
            f"--- Overall HTML Profile Scraping Process Finished. Run ID: {self.run_id} ---"
        )


# --- Example Usage Block ---
def scrape_html_profiles():
   
    # --- Configuration Check ---
    if MALT_COOKIE.startswith("YOUR_") or MALT_XSRF_TOKEN.startswith("YOUR_"):
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        print(
            "!!! WARNING: MALT_COOKIE or MALT_XSRF_TOKEN in config.py is not set.    !!!"
        )
        print(
            "!!!          Profile HTML fetching might be limited or fail.            !!!"
        )
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )

    # --- Scraper Instantiation and Configuration ---
    # The base_input_dir should point to the 'malt_fr' directory *inside* your 'Malt-fr/raw_data/'
    # For example, if your structure is Malt-fr/raw_data/malt_fr/query1/location1/...
    # then base_input_dir should be "Malt-fr/raw_data/malt_fr"

    # --- Determine the correct base_input_dir ---
    # Assuming this script is in a directory structure like:
    # ProjectRoot/
    #   Malt-fr/
    #     raw_data/
    #       malt_fr/  <-- This is the target for base_input_dir
    #         query_slug1/
    #           location_slug1/
    #             range_.../
    #               page_1.json
    #   Scrapers/
    #     malt_profile_html_scraper.py (this script)
    #     config.py

    # Construct the path dynamically based on script location or provide an absolute path.
    # This example assumes the script is run from ProjectRoot or ProjectRoot/Scrapers
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(
        script_dir
    )  # Assumes Scrapers is one level down from project root
    # If script is directly in ProjectRoot, then project_root = script_dir
    # Adjust this logic if your directory structure is different.
    # A simpler, more robust way if you always run from a specific location:
    # configured_base_input_dir = "/absolute/path/to/your/Malt-fr/raw_data/malt_fr"
    configured_base_input_dir = os.path.join(
        project_root, "Malt-fr", "raw_data", "malt_fr"
    )

    if not os.path.exists(configured_base_input_dir):
        # Fallback if the dynamic path isn't found, try a common relative path
        # (e.g., if script is run from 'Malt-fr' directory)
        configured_base_input_dir = os.path.join("raw_data", "malt_fr")
        if not os.path.exists(configured_base_input_dir):
            print(
                f"ERROR: Could not automatically determine or find base_input_dir: {configured_base_input_dir}"
            )
            print(
                "Please set `configured_base_input_dir` manually in the script or ensure the expected directory structure."
            )
            sys.exit(1)

    print(
        f"Using base input directory for JSON files: {os.path.abspath(configured_base_input_dir)}"
    )

    html_scraper = MaltProfileHtmlScraper(
        base_input_dir=configured_base_input_dir,
        num_workers=2,  # Adjust based on your network and server politeness
        # Start with a lower number like 4-8 if unsure.
    )

    # --- Run the HTML Profile Scraper ---
    html_scraper.run_overall_scraper()

    print("\n--- HTML Profile Scraping script execution finished ---")
