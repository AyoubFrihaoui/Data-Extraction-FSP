"""
MaltProfileHtmlScraper: DataOps-Oriented HTML Profile Scraper for Malt.fr (v1.1.0)

PURPOSE:
  1) Traverses a directory structure previously populated by a Malt.fr JSON API scraper.
  2) Reads `page_*.json` files from `range_<min>_<max>` subdirectories.
  3) Extracts unique freelancer profile IDs from these JSON files.
  4) Fetches the full HTML content for each unique profile ID from `https://www.malt.fr/profile/<id>`.
  5) Saves the fetched HTML content into a configurable, dedicated directory structure.
  6) Implements resuming functionality by checking for already downloaded profiles.
  7) Operates in parallel using ThreadPoolExecutor for fetching HTML pages.
  8) Implements DataOps best practices including comprehensive logging and metadata generation.

OUTPUT DIRECTORY STRUCTURE (Example - Created by this script):
  <base_html_output_dir>/  (e.g., "Malt-fr/Processed_HTML_Profiles")
    <query_slug>/
      <location_slug>/
        profiles/
          metadata_profiles.json
          <profile_id_1>.html
          ...
        scrape_html_profiles_<query_slug>_<location_slug>.log # Log file moved here

Author: @AyoubFrihaoui
Version: 1.1.0
"""

import os
import sys
import time
import json
from typing import Dict, Optional, Set, List
import uuid
import random
import logging
import platform
import requests
import re
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

# --- Configuration Import ---
try:
    from config import (
        MALT_COOKIE,
        MALT_XSRF_TOKEN,
    )  # Assuming config.py is in the same directory or PYTHONPATH
except ImportError:
    print(
        "WARNING: config.py not found or MALT_COOKIE/MALT_XSRF_TOKEN not defined. Using placeholders."
    )
    MALT_COOKIE = (
        "YOUR_MALT_COOKIE_PLACEHOLDER_CONFIG"  # Differentiate from class default
    )
    MALT_XSRF_TOKEN = "YOUR_MALT_XSRF_TOKEN_PLACEHOLDER_CONFIG"


class MaltProfileHtmlScraper:
    """
    Scrapes and saves individual Malt.fr profile HTML pages.
    Includes functionality for resuming and parallel fetching.
    """

    BASE_PROFILE_URL = "https://www.malt.fr/profile/"
    HTML_REQUEST_HEADERS = {  # Headers for requesting HTML content
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",  # Added fr as a common language on Malt
        "cache-control": "no-cache",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "priority": "u=0, i",
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    }

    def __init__(
        self,
        base_input_dir: str = os.path.join("Malt-fr", "raw_data", "malt_fr"),
        base_html_output_dir: Optional[
            str
        ] = None,  # New parameter for specifying main HTML output
        num_workers: int = 2,  # Defaulted to 2 as per your observation
        cookie: str = MALT_COOKIE,
        # xsrf_token: str = MALT_XSRF_TOKEN, # XSRF token usually not needed for GET requests
    ):
        self.base_input_dir = base_input_dir
        if not os.path.isdir(self.base_input_dir):
            print(
                f"ERROR: Base input directory for JSONs '{self.base_input_dir}' not found."
            )
            sys.exit(1)

        # If base_html_output_dir is not provided, default to a structure relative to base_input_dir
        self.base_html_output_dir = (
            base_html_output_dir
            if base_html_output_dir
            else os.path.join(
                os.path.dirname(os.path.dirname(self.base_input_dir)),
                "Processed_HTML_Profiles_Malt",
            )
        )

        os.makedirs(self.base_html_output_dir, exist_ok=True)
        print(
            f"Configured base HTML output directory: {os.path.abspath(self.base_html_output_dir)}"
        )

        self.num_workers = num_workers
        self.run_id = str(uuid.uuid4())
        self.logger = None  # Logger will be configured per context

        # Session for making requests (can help with cookies, connection pooling)
        self.session = requests.Session()
        base_session_headers = {
            "Cookie": cookie,  # Use cookie from init (config or default)
            "user-agent": self.HTML_REQUEST_HEADERS["user-agent"],
        }
        self.session.headers.update(base_session_headers)

        if cookie.startswith("YOUR_"):
            print(
                "WARNING: Using placeholder Cookie. Profile fetching might be limited/fail."
            )

    def _setup_logger_for_context(
        self, query_slug: str, location_slug: str, context_output_base_path: str
    ):
        """Configures a logger for the current query/location context."""
        # Log file will now be inside the specific query/location output directory, alongside 'profiles'
        log_file_name = f"scrape_html_profiles_{query_slug}_{location_slug}.log"
        log_file_path = os.path.join(
            context_output_base_path, log_file_name
        )  # Log in the query/location specific output folder

        logger_name = f"MaltHtmlScraper_{self.run_id[:8]}_{query_slug}_{location_slug}"
        self.logger = logging.getLogger(logger_name)
        self.logger.handlers = (
            []
        )  # Clear existing handlers for this logger instance if any
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = (
            False  # Avoid duplicate logs if root logger is also configured
        )

        fh = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s [%(module)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.info(
            f"Logger for {query_slug}/{location_slug} initialized. Log file: {log_file_path}"
        )

    def _update_run_metadata(
        self, metadata_file_path: str, run_status: str, summary_data: dict
    ):
        """Creates or updates the metadata JSON file for the current scraping run."""
        metadata = {
            "metadata_schema_version": "1.1.0",  # Version of this metadata structure
            "html_scrape_run_id": self.run_id,
            "run_start_time_utc": summary_data.get(
                "run_start_time_utc", datetime.now(timezone.utc).isoformat()
            ),
            "run_status": run_status,
            "last_updated_utc": datetime.now(timezone.utc).isoformat(),
        }
        metadata.update(summary_data)
        try:
            with open(metadata_file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            if self.logger:
                self.logger.debug(
                    f"Updated metadata {metadata_file_path} with status: {run_status}"
                )
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Failed to write metadata to {metadata_file_path}: {e}"
                )

    def _load_already_fetched_ids(self, html_output_dir_for_context: str) -> Set[str]:
        """Scans the output directory for already fetched HTML files and returns their IDs."""
        fetched_ids = set()
        if not os.path.isdir(html_output_dir_for_context):
            return fetched_ids  # Directory doesn't exist yet, so nothing fetched

        for filename in os.listdir(html_output_dir_for_context):
            if filename.endswith(".html"):
                profile_id = filename.replace(".html", "")
                fetched_ids.add(profile_id)
        if self.logger:
            self.logger.info(
                f"Found {len(fetched_ids)} already fetched profile HTMLs in {html_output_dir_for_context}"
            )
        return fetched_ids

    def _load_previous_run_metadata(self, metadata_file_path: str) -> Dict:
        """Loads metadata from a previous run, if it exists."""
        if os.path.exists(metadata_file_path):
            try:
                with open(metadata_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                if self.logger:
                    self.logger.warning(
                        f"Could not load previous metadata from {metadata_file_path}: {e}"
                    )
        return {}  # Return empty dict if no file or error

    def discover_profile_ids(self, current_location_input_path: str) -> Set[str]:
        """Scans JSON files in range_*/ directories to find unique profile IDs."""
        unique_ids = set()
        if not self.logger:
            print("Logger not ready for ID discovery.")
            return unique_ids
        self.logger.info(
            f"Discovering profile IDs from JSONs in: {current_location_input_path}"
        )
        # ... (rest of the discovery logic from your provided code, seems okay) ...
        range_dirs_found = 0
        json_files_processed = 0
        for item_name in os.listdir(current_location_input_path):
            item_path = os.path.join(current_location_input_path, item_name)
            if os.path.isdir(item_path) and item_name.startswith("range_"):
                range_dirs_found += 1
                for page_file_name in os.listdir(item_path):
                    if page_file_name.startswith("page_") and page_file_name.endswith(
                        ".json"
                    ):
                        page_file_path = os.path.join(item_path, page_file_name)
                        try:
                            with open(page_file_path, "r", encoding="utf-8") as f:
                                data = json.load(f)
                            for profile in data.get("profiles", []):
                                if profile_id := profile.get("id"):
                                    unique_ids.add(profile_id)
                            json_files_processed += 1
                        except Exception as e:
                            self.logger.error(
                                f"Error processing JSON {page_file_path}: {e}"
                            )
        self.logger.info(
            f"ID Discovery: {len(unique_ids)} unique IDs from {json_files_processed} JSONs in {range_dirs_found} ranges."
        )
        return unique_ids

    def _fetch_single_profile_html_worker(
        self, profile_id: str, html_output_dir: str
    ) -> tuple[str, bool, Optional[str]]:
        """Worker to fetch and save HTML for one profile."""
        profile_url = f"{self.BASE_PROFILE_URL}{profile_id}"
        html_file_path = os.path.join(html_output_dir, f"{profile_id}.html")

        # Use the request structure you found working
        payload = {}
        headers = {  # Simplified headers, ensure MALT_COOKIE is correctly passed via session or here
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,ar;q=0.8",  # You can cycle or make this more dynamic if needed
            "priority": "u=0, i",
            "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self.HTML_REQUEST_HEADERS["user-agent"],
            "Cookie": self.session.headers.get("Cookie"),  # Use cookie from session
        }
        try:
            self.logger.debug(f"Fetching HTML for {profile_id} from {profile_url} with custom request")
            response = requests.request("GET", profile_url, headers=headers, data=payload, timeout=40) # Your direct request

            # Alternative using the session object, which is generally preferred:
            request_headers_for_session = self.HTML_REQUEST_HEADERS.copy()
            # The session already has base UA and Cookie. Referer can be useful.


            # request_headers_for_session["referer"] = (
            #     "https://www.malt.fr/s?"  # Generic search referer
            # )
            # response = self.session.get(
            #     profile_url, headers=request_headers_for_session, timeout=45
            # )

            response.raise_for_status()  # Will raise an HTTPError for bad responses (4XX or 5XX)

            # Save as binary content to avoid encoding issues during write
            with open(html_file_path, "w", encoding="utf-8") as f:
                f.write(response.text)


            if self.logger:
                self.logger.debug(
                    f"Successfully saved HTML (binary) for profile {profile_id}"
                )

            time.sleep(
                round(random.uniform(1.9, 3.2), 3)
            )  # Slightly reduced sleep as per your observation
            return profile_id, True, None
        except requests.exceptions.Timeout:
            msg = f"Timeout fetching {profile_id}"
            if self.logger:
                self.logger.warning(msg)
            return profile_id, False, msg
        except requests.exceptions.HTTPError as http_err:
            msg = f"HTTP error {http_err.response.status_code} for {profile_id}: {http_err}"
            if self.logger:
                self.logger.warning(msg)
            return profile_id, False, msg
        except Exception as e:
            msg = f"Generic error fetching {profile_id}: {str(e)}"
            if self.logger:
                self.logger.error(msg, exc_info=False)
            return profile_id, False, msg

    def process_query_location(
        self, query_slug: str, location_slug: str, global_base_html_output_dir: str
    ):
        """Processes one query/location context: discovers IDs, fetches HTMLs, saves, and creates metadata."""

        # Path for input JSONs for this specific query/location
        current_location_input_path = os.path.join(
            self.base_input_dir, query_slug, location_slug
        )
        if not os.path.isdir(current_location_input_path):
            print(f"Skipping: Input JSON path not found {current_location_input_path}")
            return

        # Path for output HTMLs & logs for this specific query/location
        # e.g., <global_base_html_output_dir>/developpeur_python/paris/
        context_specific_output_base = os.path.join(
            global_base_html_output_dir, query_slug, location_slug
        )
        html_output_dir_for_context = os.path.join(
            context_specific_output_base, "profiles"
        )
        os.makedirs(
            html_output_dir_for_context, exist_ok=True
        )  # Ensure 'profiles' subdirectory exists

        self._setup_logger_for_context(
            query_slug, location_slug, context_specific_output_base
        )  # Log file in context_specific_output_base
        self.logger.info(
            f"--- HTML Scraping START: {query_slug}/{location_slug} --- Output: {html_output_dir_for_context}"
        )

        run_metadata_file = os.path.join(
            html_output_dir_for_context, "metadata_profiles.json"
        )
        previous_metadata = self._load_previous_run_metadata(run_metadata_file)

        run_start_time = time.time()
        summary_data = {
            "query_slug": query_slug,
            "location_slug": location_slug,
            "run_start_time_utc": datetime.now(timezone.utc).isoformat(),
            "status_details": "ID Discovery & Resume Check",
            "ids_failed_this_run": [],  # List of IDs that fail in *this specific* execution
            "previously_failed_ids_count_estimate": previous_metadata.get(
                "html_profiles_failed", 0
            ),  # Estimate from last run
        }
        self._update_run_metadata(
            run_metadata_file, "processing_discovery", summary_data
        )

        all_known_profile_ids = self.discover_profile_ids(current_location_input_path)
        if not all_known_profile_ids:
            self.logger.info("No profile IDs found from JSONs. Nothing to fetch.")
            summary_data.update(
                {
                    "total_unique_ids_found_from_json": 0,
                    "html_profiles_to_attempt": 0,
                    "status_details": "No IDs in JSONs",
                }
            )
            self._update_run_metadata(
                run_metadata_file, "success_no_ids_found", summary_data
            )
            return

        summary_data["total_unique_ids_found_from_json"] = len(all_known_profile_ids)

        already_fetched_ids = self._load_already_fetched_ids(
            html_output_dir_for_context
        )
        ids_to_attempt_this_run = list(
            all_known_profile_ids - already_fetched_ids
        )  # Use list for ordered processing if desired
        random.shuffle(
            ids_to_attempt_this_run
        )  # Shuffle to vary request patterns slightly

        summary_data.update(
            {
                "total_already_fetched_ids": len(already_fetched_ids),
                "html_profiles_to_attempt_this_run": len(ids_to_attempt_this_run),
                "status_details": "HTML Fetching In Progress",
            }
        )
        self._update_run_metadata(
            run_metadata_file, "processing_html_fetching", summary_data
        )

        if not ids_to_attempt_this_run:
            self.logger.info(
                "All known profile IDs have already been fetched. Nothing new to process."
            )
            summary_data.update(
                {
                    "html_profiles_fetched_this_run": 0,
                    "html_profiles_failed_this_run": 0,
                    "run_end_time_utc": datetime.now(timezone.utc).isoformat(),
                    "duration_seconds_this_run": round(time.time() - run_start_time, 2),
                    "status_details": "Completed - No new IDs to fetch",
                }
            )
            self._update_run_metadata(
                run_metadata_file, "success_all_previously_fetched", summary_data
            )
            return

        self.logger.info(
            f"Total IDs from JSONs: {len(all_known_profile_ids)}. Already fetched: {len(already_fetched_ids)}. To attempt now: {len(ids_to_attempt_this_run)}."
        )

        newly_fetched_count = 0
        failed_this_run_count = 0
        failed_ids_this_run_list = (
            []
        )  # Specific IDs that failed in this current execution

        total_to_process_this_run = len(ids_to_attempt_this_run)

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures_map = {
                executor.submit(
                    self._fetch_single_profile_html_worker,
                    profile_id,
                    html_output_dir_for_context,
                ): profile_id
                for profile_id in ids_to_attempt_this_run
            }
            for i, future in enumerate(as_completed(futures_map)):
                profile_id = futures_map[future]
                try:
                    _, success, error_msg = future.result()
                    if success:
                        newly_fetched_count += 1
                    else:
                        failed_this_run_count += 1
                        failed_ids_this_run_list.append(profile_id)
                        if error_msg:
                            self.logger.debug(
                                f"Failed HTML for {profile_id}: {error_msg}"
                            )
                except Exception as e_future:
                    failed_this_run_count += 1
                    failed_ids_this_run_list.append(profile_id)
                    self.logger.error(
                        f"Error processing future for {profile_id}: {e_future}",
                        exc_info=True,
                    )

                # Updated Progress Log
                total_processed_so_far = i + 1
                # Estimate of total overall failures: previous known failures + current failures for IDs NOT yet successfully fetched
                # This is a rough estimate as it doesn't perfectly track which of the previous failures are being retried now.
                estimated_total_failures = (
                    summary_data["previously_failed_ids_count_estimate"]
                    + failed_this_run_count
                )

                if (
                    total_processed_so_far % 20 == 0
                    or total_processed_so_far == total_to_process_this_run
                ):  # Log every 20 or at the end
                    self.logger.info(
                        f"Progress: Processed {total_processed_so_far}/{total_to_process_this_run} in current batch. "
                        f"(Total Known IDs: {len(all_known_profile_ids)}, "
                        f"Already Fetched: {len(already_fetched_ids)}, "
                        f"Newly Fetched This Run: {newly_fetched_count}, "
                        f"Failed This Run: {failed_this_run_count}, "
                        f"Est. Total Failures: {estimated_total_failures})"
                    )

        run_end_time = time.time()
        duration_this_run = round(run_end_time - run_start_time, 2)
        final_status = (
            "completed_with_errors" if failed_this_run_count > 0 else "success"
        )
        if (
            newly_fetched_count == 0
            and failed_this_run_count == total_to_process_this_run
            and total_to_process_this_run > 0
        ):
            final_status = "error_all_attempted_failed_this_run"

        summary_data.update(
            {
                "html_profiles_fetched_this_run": newly_fetched_count,
                "html_profiles_failed_this_run": failed_this_run_count,
                "ids_failed_this_run": failed_ids_this_run_list,  # Store IDs that failed in this run
                "total_html_files_in_output_dir": len(
                    self._load_already_fetched_ids(html_output_dir_for_context)
                ),  # Recount after run
                "run_end_time_utc": datetime.now(timezone.utc).isoformat(),
                "duration_seconds_this_run": duration_this_run,
                "status_details": "Run completed",
            }
        )
        self._update_run_metadata(run_metadata_file, final_status, summary_data)
        self.logger.info(
            f"HTML Fetching for current batch complete. Newly Fetched: {newly_fetched_count}, Failed This Run: {failed_this_run_count}."
        )
        self.logger.info(
            f"Total duration for {query_slug}/{location_slug} this run: {duration_this_run}s."
        )
        self.logger.info(
            f"--- HTML Profile Scraping FINISHED for: {query_slug}/{location_slug} ---"
        )

    def run_overall_scraper(self):
        """Main orchestrator: iterates through query/location directories."""
        print(f"Starting Overall HTML Profile Scraper. Run ID: {self.run_id}")
        print(f"Base input (JSONs): {os.path.abspath(self.base_input_dir)}")
        print(f"Base output (HTMLs): {os.path.abspath(self.base_html_output_dir)}")

        if not os.path.exists(self.base_input_dir):
            print(
                f"CRITICAL: Base input directory '{self.base_input_dir}' non-existent."
            )
            return

        query_slug_dirs = [
            d
            for d in os.listdir(self.base_input_dir)
            if os.path.isdir(os.path.join(self.base_input_dir, d))
        ]
        if not query_slug_dirs:
            print(f"No query directories found in '{self.base_input_dir}'.")
            return
        print(f"Found {len(query_slug_dirs)} query directories: {query_slug_dirs}")

        for query_slug in query_slug_dirs:
            query_input_path = os.path.join(self.base_input_dir, query_slug)
            location_slug_dirs = [
                d
                for d in os.listdir(query_input_path)
                if os.path.isdir(os.path.join(query_input_path, d))
            ]
            if not location_slug_dirs:
                print(
                    f"No location directories in '{query_input_path}' for query '{query_slug}'."
                )
                continue

            print(
                f"  Processing query '{query_slug}': {len(location_slug_dirs)} location(s)."
            )
            for location_slug in location_slug_dirs:
                self.process_query_location(
                    query_slug, location_slug, self.base_html_output_dir
                )
                time.sleep(
                    random.uniform(4, 7)
                )  # Small delay between different location contexts
        print(
            f"--- Overall HTML Profile Scraping Process Finished. Run ID: {self.run_id} ---"
        )


# --- Example Usage ---
def main():
    # --- Dynamic Base Input Directory (adjust if needed) ---
    # This tries to find Malt-fr/raw_data/malt_fr relative to the script's location
    # You might want to hardcode this or pass it as a command-line argument for robustness
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root_guess = os.path.dirname(script_dir)  # If script is in Scrapers/
        default_base_input_dir = os.path.join(
            project_root_guess, "raw_data", "malt_fr"
        )
        if not os.path.isdir(default_base_input_dir):  # Try another common case
            default_base_input_dir = os.path.join(
                script_dir, "Malt-fr", "raw_data", "malt_fr"
            )
            if not os.path.isdir(
                default_base_input_dir
            ):  # Fallback to current dir + relative path
                default_base_input_dir = os.path.join(
                    os.getcwd(), "Malt-fr", "raw_data", "malt_fr"
                )
    except NameError:  # __file__ not defined (e.g. in interactive session)
        default_base_input_dir = os.path.join(
            os.getcwd(), "Malt-fr", "raw_data", "malt_fr"
        )

    print(
        f"Attempting to use base input directory for JSONs: {os.path.abspath(default_base_input_dir)}"
    )
    if not os.path.isdir(default_base_input_dir):
        print(
            f"ERROR: Default base_input_dir '{default_base_input_dir}' not found. Please check the path."
        )
        print(
            "You might need to manually set 'configured_base_input_dir' or 'configured_html_output_dir'."
        )
        return

    # --- Configuration for the scraper ---
    # Option 1: Output HTMLs relative to the JSON input structure (in a "Processed_HTML_Profiles_Malt" folder)
    # configured_html_output_dir = None # Let the class default handle it

    # Option 2: Specify a completely custom absolute path for HTML outputs
    configured_html_output_dir = (
        "H:/Data Extraction FSP/Malt-fr/raw_data/malt_fr/developpeur/en_télétravail/profiles"  # Example
    )
    print(
        f"HTML output will be saved under: {os.path.abspath(configured_html_output_dir)}"
    )

    scraper = MaltProfileHtmlScraper(
        base_input_dir=default_base_input_dir,
        base_html_output_dir=configured_html_output_dir,  # Pass your desired main output path here
        num_workers=1,  # Your preferred number of workers
        cookie=MALT_COOKIE,  # From config.py or placeholder
    )
    scraper.run_overall_scraper()
    print("\n--- HTML Profile Scraping Script Execution Finished ---")


if __name__ == "__main__":
    main()
