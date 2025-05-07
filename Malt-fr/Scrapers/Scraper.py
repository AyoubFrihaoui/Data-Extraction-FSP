"""
MaltScraper: DataOps-Oriented Web Scraping Pipeline for Malt.fr

PURPOSE:
  1) Scrape freelancer profile data (primarily IDs) from Malt.fr based on a search query,
     price ranges, and location.
  2) Paginate through search results up to a defined maximum page limit for each price range.
  3) Showcase a DataOps approach by creating metadata files (JSON) in each directory under raw_data/
     to record run details, error states, and an "api_request" field (URL and headers, excluding cookies).
  4) Capture all console output in a dedicated log file using Python’s logging module.

DIRECTORY STRUCTURE:
  raw_data/
    malt_fr/
      metadata_malt_fr.json
      <query_term_slug>/
        metadata_<query_term_slug>.json
        <location_cleaned>/
          metadata_<location_cleaned>.json
          range_<price_min>_<price_max>/  (or range_<price_min>_inf for open-ended max)
            page_1.json
            page_2.json
            ...
            metadata_range_<price_min>_<price_max_or_inf>.json

LOGGING:
  - A single log file (scrape_malt_<query_term_slug>.log) is placed in the <location_cleaned> directory.
  - All print statements are replaced by logger calls, which also go to console.

METADATA & ERROR HANDLING:
  - Each directory has a minimal or detailed metadata file named metadata_<directory_name>.json.
  - On errors, we store scrape_status = "error", plus error_stage and error_message for debugging.
  - The "api_request" field in range metadata includes the request URL and headers (minus cookies).

'DECEPTION' FIELD:
  - Each metadata file has a "deception" field, explaining how future LLMs or data engineers should
    interpret the file’s structure and purpose.

PRICE RANGES:
  - Scrapes in increments of 50 EUR, starting from a base minimum (e.g., 125 EUR).
  - The final range is from a specified minimum (e.g., 800 EUR) to infinity (no maxPrice).

DISCLAIMER:
  - This code is for educational purposes. Excessive scraping can lead to IP blocking or other actions.
  - Always respect the site's Terms of Service. Use responsibly.

Author: Your Name / Inspired by @AyoubFrihaoui
Version: 1.0.0
"""

import os
import sys
import time
import json
import uuid
import random
import logging
import platform
import requests
import re
from urllib.parse import urlencode, quote_plus
from datetime import datetime
from typing import Tuple, List, Optional, Dict, Any

# Attempt to import sensitive credentials from config.py
try:
    from config import MALT_COOKIE, MALT_XSRF_TOKEN
except ImportError:
    print(
        "ERROR: config.py not found or MALT_COOKIE/MALT_XSRF_TOKEN not set. "
        "Please create config.py with your credentials."
    )
    MALT_COOKIE = "YOUR_MALT_COOKIE_MUST_BE_SET_IN_CONFIG_PY"
    MALT_XSRF_TOKEN = "YOUR_MALT_XSRF_TOKEN_MUST_BE_SET_IN_CONFIG_PY"
    # sys.exit(1) # Optionally exit if config is crucial


class MaltScraper:
    """
    A pipeline to scrape freelancer profile data from Malt.fr using their search API.
    Incorporates DataOps practices (metadata JSON files) and logs all console output to a file.
    """

    BASE_API_URL = "https://www.malt.fr/search/api/profiles"

    # Base HTTP headers (Cookie and X-XSRF-TOKEN will be added from config)
    BASE_HEADERS = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",  # Added fr for malt.fr context
        "baggage": "sentry-environment=production,sentry-release=47c30717f2cc846ffd6376ea06ba8b4686d76ff4,sentry-public_key=f56cf160abf7427a85ee77a9a381dabb,sentry-trace_id=450eec62a9874e5786686ad91dabad25",  # Example, may change
        "cache-control": "no-cache",  # Explicitly no-cache
        "priority": "u=1, i",
        "referer": "https://www.malt.fr/s?q=developpeur&remoteAllowed=true&location=En+t%C3%A9l%C3%A9travail+(France)",  # Generic referer
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',  # Update if browser changes
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',  # Or your platform
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sentry-trace": "450eec62a9874e5786686ad91dabad25-ad5d0c1b639cb53c",  # Example, may change
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",  # Keep updated
        "x-app-version": "47c30717",  # Example, may change
        # 'x-xsrf-token' will be added dynamically
        # 'Cookie' will be added dynamically
    }

    def __init__(
        self,
        query_term: str = "developpeur",
        location_name: str = "En télétravail (France)",  # Human-readable for dir name
        remote_allowed: bool = True,
        initial_min_price: int = 125,
        price_step: int = 50,
        final_min_price_for_open_range: int = 800,
        max_pages_per_range: int = 500,  # Malt allows up to 500 pages
        cookie: str = MALT_COOKIE,
        xsrf_token: str = MALT_XSRF_TOKEN,
    ):
        """
        :param query_term: Search term, e.g., "developpeur"
        :param location_name: Human-readable location, e.g., "En télétravail (France)"
        :param remote_allowed: Boolean, whether to search for remote-only profiles
        :param initial_min_price: Starting minimum price for the first range, e.g., 125
        :param price_step: Increment for price ranges, e.g., 50
        :param final_min_price_for_open_range: Min price for the last range (max is infinity), e.g., 800
        :param max_pages_per_range: Max number of pages to scrape per price range
        :param cookie: Malt.fr cookie string
        :param xsrf_token: Malt.fr XSRF token
        """
        self.query_term = query_term
        self.query_term_slug = self._slugify(query_term)  # For directory naming
        self.location_name = location_name
        # Malt uses URL-encoded location in query params, but we want a clean dir name
        self.location_cleaned_slug = self._slugify(
            location_name.split("(")[0].strip()
        )  # e.g. "en_teletravail"

        self.remote_allowed_str = str(remote_allowed).lower()
        self.initial_min_price = initial_min_price
        self.price_step = price_step
        self.final_min_price_for_open_range = final_min_price_for_open_range
        self.max_pages_per_range = max_pages_per_range

        self.session = requests.Session()
        self.headers = self.BASE_HEADERS.copy()
        self.headers["Cookie"] = cookie
        self.headers["x-xsrf-token"] = xsrf_token
        self.session.headers.update(self.headers)

        self.run_id = str(uuid.uuid4())

        # Directory structure: raw_data/malt_fr/<query_term_slug>/<location_cleaned_slug>/
        self.base_dir = os.path.join(
            "raw_data",
            "malt_fr",  # Site name
            self.query_term_slug,
            self.location_cleaned_slug,
        )
        os.makedirs(self.base_dir, exist_ok=True)

        self._setup_logging()

        # Initialize stub metadata for each directory in the path
        self._init_directory_with_metadata(
            os.path.join("raw_data"), "Root raw_data directory"
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", "malt_fr"), "Site: malt.fr"
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", "malt_fr", self.query_term_slug),
            f"Query term: {self.query_term}",
        )
        self._init_directory_with_metadata(
            self.base_dir, f"Location: {self.location_name}"
        )
        self.logger.info(
            f"MaltScraper initialized for query '{self.query_term}' at '{self.location_name}'"
        )
        if (
            cookie == "YOUR_MALT_COOKIE_MUST_BE_SET_IN_CONFIG_PY"
            or xsrf_token == "YOUR_MALT_XSRF_TOKEN_MUST_BE_SET_IN_CONFIG_PY"
        ):
            self.logger.warning(
                "Using placeholder Cookie/XSRF-Token. Scraper will likely fail. Update config.py."
            )

    def _slugify(self, text: str) -> str:
        """Helper to create a clean string for directory names."""
        text = text.lower()
        text = re.sub(
            r"[^\w\s-]", "", text
        )  # Remove non-alphanumeric (excluding spaces, hyphens)
        text = re.sub(r"[-\s]+", "_", text)  # Replace spaces/hyphens with underscore
        return text.strip("_")

    def _setup_logging(self):
        """
        Configure a logger to output to both console and a file named
        scrape_malt_<query_term_slug>.log inside the location_cleaned_slug directory.
        """
        log_file = os.path.join(
            self.base_dir, f"scrape_malt_{self.query_term_slug}.log"
        )

        self.logger = logging.getLogger(f"{self.run_id}_MaltScraper")
        self.logger.setLevel(logging.DEBUG)
        # Avoid adding handlers if they already exist (e.g. if class is instantiated multiple times)
        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)

            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        self.logger.info(f"Log file initialized at: {log_file}")

    def _init_directory_with_metadata(self, dir_path: str, description: str) -> None:
        os.makedirs(dir_path, exist_ok=True)
        dir_name = os.path.basename(dir_path)
        if not dir_name:  # Handle case where dir_path might be "." or "raw_data/"
            dir_name = os.path.basename(os.path.abspath(dir_path))

        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")

        metadata_stub = {
            "deception": (
                "This metadata file is used by future LLMs or data engineers to understand "
                "the structure and purpose of the data in this directory."
            ),
            "directory_name": dir_name,
            "creation_time": datetime.utcnow().isoformat() + "Z",
            "scrape_status": "initialized",
            "run_id": self.run_id,
            "description": description,
        }
        # Only write if file doesn't exist or to avoid overwriting useful existing info
        # For this script's logic, overwriting initial stubs is fine if run_id changes.
        # If multiple scrapers share dirs, more sophisticated logic might be needed.
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_stub, f, ensure_ascii=False, indent=2)

    def _update_directory_metadata(
        self,
        dir_path: str,
        scrape_status: str,
        error_message: Optional[str] = None,
        error_stage: Optional[str] = None,
        extra_data: Optional[dict] = None,
    ) -> None:
        dir_name = os.path.basename(dir_path)
        if not dir_name:
            dir_name = os.path.basename(os.path.abspath(dir_path))
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")

        if os.path.isfile(metadata_file):
            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        else:
            metadata = {
                "deception": (
                    "This metadata file is used by future LLMs or data engineers to understand "
                    "the structure and purpose of the data in this directory."
                ),
                "directory_name": dir_name,
                "creation_time": datetime.utcnow().isoformat() + "Z",
                "run_id": self.run_id,  # Ensure run_id is present
            }

        metadata["scrape_status"] = scrape_status
        metadata["last_updated_time"] = datetime.utcnow().isoformat() + "Z"
        if error_message:
            metadata["error_message"] = error_message
        if error_stage:
            metadata["error_stage"] = error_stage

        # For specific run_id tracking if multiple runs update same dir metadata
        if "runs" not in metadata:
            metadata["runs"] = {}
        if self.run_id not in metadata["runs"]:
            metadata["runs"][self.run_id] = {}

        metadata["runs"][self.run_id]["scrape_status"] = scrape_status
        if error_message:
            metadata["runs"][self.run_id]["error_message"] = error_message
        if error_stage:
            metadata["runs"][self.run_id]["error_stage"] = error_stage

        if extra_data:
            for k, v in extra_data.items():
                metadata[k] = v
                metadata["runs"][self.run_id][k] = v  # Also log to specific run

        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def _generate_price_ranges(self) -> List[Tuple[int, Optional[int]]]:
        """
        Generates price ranges:
        (125, 174), (175, 224), ..., up to (final_min_price_for_open_range - price_step, final_min_price_for_open_range - 1)
        And finally (final_min_price_for_open_range, None)
        """
        ranges: List[Tuple[int, Optional[int]]] = []
        current_min = self.initial_min_price

        while current_min < self.final_min_price_for_open_range:
            current_max = current_min + self.price_step - 1
            ranges.append((current_min, current_max))
            current_min += self.price_step

        # Add the final open-ended range
        ranges.append((self.final_min_price_for_open_range, None))
        self.logger.info(f"Generated {len(ranges)} price ranges to scrape.")
        return ranges

    def _build_api_url(
        self, page: int, price_min: int, price_max: Optional[int]
    ) -> str:
        """Constructs the API URL with query parameters."""
        params: Dict[str, Any] = {
            "q": self.query_term,
            "remoteAllowed": self.remote_allowed_str,
            "location": self.location_name,  # Malt expects the human-readable, URL-encoded string
            "p": page,
            "minPrice": price_min,
            # searchid: can be dynamic or fixed. Let's try with a new one per run or omit if not strictly needed.
            # For stability, might be better to use a fixed one if identified or generate a consistent one.
            # For now, generating one:
            "searchid": str(uuid.uuid4()).replace("-", "")[
                :24
            ],  # Example: Malt uses 24 char hex
        }
        if price_max is not None:
            params["maxPrice"] = price_max

        # The requests library handles URL encoding of query parameters automatically
        return f"{self.BASE_API_URL}?{urlencode(params, quote_via=quote_plus)}"

    def _fetch_profiles_for_range(self, price_min: int, price_max: Optional[int]):
        """
        Paginates through results for the given price range, storing JSON pages.
        """
        range_label = (
            f"{price_min}_{price_max}" if price_max is not None else f"{price_min}_inf"
        )
        range_dir_name = f"range_{range_label}"
        range_dir = os.path.join(self.base_dir, range_dir_name)
        os.makedirs(range_dir, exist_ok=True)

        self._init_directory_with_metadata(
            range_dir,
            f"Price range: {price_min} EUR to {price_max if price_max else 'infinity'} EUR",
        )

        start_time = time.time()
        page_count = 0
        total_profiles_in_range = 0
        scrape_status = "success"  # Assume success initially
        error_message = None
        error_stage = None

        # For metadata api_request - capture one example URL (first page)
        # Headers are already in self.session.headers
        first_page_url_for_metadata = ""

        self.logger.info(
            f"Fetching data for price range [{price_min}, {price_max if price_max else 'Inf'}] EUR..."
        )

        try:
            for page_num in range(1, self.max_pages_per_range + 1):
                page_count = page_num
                request_url = self._build_api_url(page_num, price_min, price_max)
                if page_num == 1:
                    first_page_url_for_metadata = request_url

                self.logger.debug(f"  Requesting: {request_url}")

                try:
                    response = self.session.get(
                        request_url, timeout=30
                    )  # Added timeout
                    response.raise_for_status()  # Will raise HTTPError for bad responses (4XX, 5XX)
                    data = response.json()
                except requests.exceptions.Timeout:
                    scrape_status = "error"
                    error_message = (
                        f"Timeout on page {page_num} for range {range_label}."
                    )
                    error_stage = f"{range_dir_name}_page_{page_num}_request"
                    self.logger.error(error_message)
                    break  # Stop this range on timeout
                except requests.exceptions.HTTPError as http_err:
                    scrape_status = "error"
                    error_message = f"HTTP error {http_err.response.status_code} on page {page_num}: {http_err}"
                    error_stage = f"{range_dir_name}_page_{page_num}_http"
                    self.logger.error(error_message)
                    # Malt might return 403 if cookie/token is bad or rate limited
                    if http_err.response.status_code == 403:
                        self.logger.error(
                            "Received 403 Forbidden. Check Cookie, XSRF-Token, and IP reputation."
                        )
                    break  # Stop this range on HTTP error
                except json.JSONDecodeError as json_err:
                    scrape_status = "error"
                    error_message = f"JSON decode error on page {page_num}: {json_err}. Response text: {response.text[:200]}"
                    error_stage = f"{range_dir_name}_page_{page_num}_json"
                    self.logger.error(error_message)
                    break  # Stop this range on JSON error
                except Exception as e_req:  # Catch any other request-related error
                    scrape_status = "error"
                    error_message = f"Generic request error on page {page_num}: {e_req}"
                    error_stage = f"{range_dir_name}_page_{page_num}_generic_request"
                    self.logger.error(error_message)
                    break

                profiles = data.get("profiles", [])
                num_profiles_on_page = len(profiles)
                total_profiles_in_range += num_profiles_on_page

                page_file = os.path.join(range_dir, f"page_{page_num}.json")
                with open(page_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                self.logger.info(
                    f"  - Page {page_num}: {num_profiles_on_page} profiles. Saved to {page_file}"
                )

                if (
                    num_profiles_on_page == 0 and page_num > 1
                ):  # page 1 can be empty if range has no results
                    self.logger.info(
                        f"  - No more profiles found for this range. Stopping pagination."
                    )
                    break

                if page_num == self.max_pages_per_range:
                    self.logger.info(
                        f"  - Reached max_pages_per_range ({self.max_pages_per_range}). Stopping."
                    )
                    break

                # Random sleep to be polite
                time.sleep(round(random.uniform(1.5, 3.5), 2))  # Increased sleep

        except Exception as e_outer:  # Catch errors in the loop setup itself
            scrape_status = "error"
            error_message = f"Outer loop exception for range {range_label}: {e_outer}"
            error_stage = f"{range_dir_name}_loop_setup"
            self.logger.error(error_message, exc_info=True)

        end_time = time.time()
        duration_seconds = round(end_time - start_time, 2)

        self.logger.info(
            f"Finished range [{price_min}, {price_max if price_max else 'Inf'}] with status='{scrape_status}'. "
            f"Total pages processed: {page_count}, total profiles fetched: {total_profiles_in_range}."
        )

        safe_headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ["cookie", "x-xsrf-token"]
        }

        scraper_info = {
            "python_version": sys.version.split("\n")[0],
            "platform": platform.platform(),
            "scraper_version": "1.0.0",  # Version of this scraper script
        }

        extra_meta = {
            "start_time_iso": datetime.utcfromtimestamp(start_time).isoformat() + "Z",
            "end_time_iso": datetime.utcfromtimestamp(end_time).isoformat() + "Z",
            "duration_seconds": duration_seconds,
            "pages_scraped_in_range": page_count,  # More specific than total_pages
            "total_profiles_in_range": total_profiles_in_range,
            "api_request_details": {  # Renamed from api_request for clarity
                "base_url": self.BASE_API_URL,
                "example_full_url_first_page": first_page_url_for_metadata,
                "headers_sent (excluding sensitive)": safe_headers,
                "query_parameters_structure": {
                    "q": self.query_term,
                    "remoteAllowed": self.remote_allowed_str,
                    "location": self.location_name,
                    "p": "<page_number>",
                    "minPrice": price_min,
                    "maxPrice": (
                        price_max if price_max is not None else "omitted (infinity)"
                    ),
                    "searchid": "<dynamic_search_id>",
                },
            },
            "scraper_environment": scraper_info,  # Renamed from scraper_info
        }

        self._update_directory_metadata(
            dir_path=range_dir,
            scrape_status=scrape_status,
            error_message=error_message,
            error_stage=error_stage,
            extra_data=extra_meta,
        )
        if scrape_status == "error":
            return False  # Indicate failure for this range
        return True  # Indicate success for this range

    def run(self):
        """
        Main entry point:
          1) Mark top-level query/location directory as "processing".
          2) Generate price ranges.
          3) For each price range, call _fetch_profiles_for_range.
          4) Update top-level directory status based on overall success.
        """
        self.logger.info(
            f"Starting Malt.fr scraping run ID: {self.run_id}\n"
            f"Query: '{self.query_term}', Location: '{self.location_name}'\n"
            f"Price Config: Start {self.initial_min_price}, Step {self.price_step}, Open Range From {self.final_min_price_for_open_range}\n"
            f"Max Pages per Range: {self.max_pages_per_range}"
        )

        # Construct a dictionary of run parameters that are safe to serialize
        run_params_to_log = {
            "query_term": self.query_term,
            "query_term_slug": self.query_term_slug,
            "location_name": self.location_name,
            "location_cleaned_slug": self.location_cleaned_slug,
            "remote_allowed_str": self.remote_allowed_str,
            "initial_min_price": self.initial_min_price,
            "price_step": self.price_step,
            "final_min_price_for_open_range": self.final_min_price_for_open_range,
            "max_pages_per_range": self.max_pages_per_range,
            # Avoid logging self.headers, self.session, self.logger, full cookie/token, etc.
        }
        self._update_directory_metadata(
            self.base_dir,
            scrape_status="processing_started",
            extra_data={"run_parameters": run_params_to_log}
        )

        overall_success = True
        try:
            price_ranges = self._generate_price_ranges()

            for i, (p_min, p_max) in enumerate(price_ranges):
                self.logger.info(
                    f"--- Processing Price Range {i+1}/{len(price_ranges)}: [{p_min} - {p_max if p_max else 'Inf'}] ---"
                )
                range_success = self._fetch_profiles_for_range(p_min, p_max)
                if not range_success:
                    overall_success = (
                        False  # If any range fails, mark overall as partial/error
                    )
                    self.logger.warning(
                        f"Price range [{p_min} - {p_max if p_max else 'Inf'}] encountered errors."
                    )

                # Add a longer delay between price ranges if desired
                if i < len(price_ranges) - 1:  # Not the last range
                    inter_range_delay = round(random.uniform(5, 10), 1)
                    self.logger.info(
                        f"Sleeping for {inter_range_delay}s before next price range..."
                    )
                    time.sleep(inter_range_delay)

            final_status = "success" if overall_success else "completed_with_errors"
            self._update_directory_metadata(self.base_dir, scrape_status=final_status)
            self.logger.info(
                f"Malt.fr scraping run {self.run_id} finished with status: {final_status}!"
            )

        except Exception as e_main:
            self.logger.error(
                f"Critical error in main run execution: {e_main}", exc_info=True
            )
            self._update_directory_metadata(
                self.base_dir,
                scrape_status="error",
                error_message=str(e_main),
                error_stage="main_run_execution",
            )
            # Re-raise might be desired depending on how this script is called
            # raise


# ------------------------------------------------------------------------------
# EXAMPLE USAGE
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Ensure MALT_COOKIE and MALT_XSRF_TOKEN are set in config.py or passed directly
    if (
        MALT_COOKIE == "YOUR_MALT_COOKIE_MUST_BE_SET_IN_CONFIG_PY"
        or MALT_XSRF_TOKEN == "YOUR_MALT_XSRF_TOKEN_MUST_BE_SET_IN_CONFIG_PY"
    ):
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        print(
            "!!! CRITICAL: MALT_COOKIE or MALT_XSRF_TOKEN is not set in config.py. !!!"
        )
        print(
            "!!! The scraper will likely fail. Please update config.py.              !!!"
        )
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        # sys.exit("Configuration error: Cookie/XSRF token not set.") # Force exit

    scraper = MaltScraper(
        query_term="développeur python",  # Be specific if possible
        location_name="En télétravail (France)",
        remote_allowed=True,
        initial_min_price=125,
        price_step=50,  # This creates ranges like 125-174, 175-224, etc.
        final_min_price_for_open_range=800,  # Last range will be 800 to infinity
        max_pages_per_range=5,  # Lower for testing; Malt allows up to 500.
        # cookie=MALT_COOKIE, # Loaded by default from config.py
        # xsrf_token=MALT_XSRF_TOKEN # Loaded by default from config.py
    )
    scraper.run()

    # Example for a different query or location
    # scraper_lyon = MaltScraper(
    #     query_term="consultant seo",
    #     location_name="Lyon", # For non-remote, just city name
    #     remote_allowed=False,
    #     initial_min_price=200,
    #     price_step=100,
    #     final_min_price_for_open_range=1000,
    #     max_pages_per_range=3
    # )
    # scraper_lyon.run()

def run_scraper():
    if (
        MALT_COOKIE == "YOUR_MALT_COOKIE_MUST_BE_SET_IN_CONFIG_PY"
        or MALT_XSRF_TOKEN == "YOUR_MALT_XSRF_TOKEN_MUST_BE_SET_IN_CONFIG_PY"
    ):
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        print(
            "!!! CRITICAL: MALT_COOKIE or MALT_XSRF_TOKEN is not set in config.py. !!!"
        )
        print(
            "!!! The scraper will likely fail. Please update config.py.              !!!"
        )
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        # sys.exit("Configuration error: Cookie/XSRF token not set.") # Force exit

    scraper = MaltScraper(
        query_term="développeur python",  # Be specific if possible
        location_name="En télétravail (France)",
        remote_allowed=True,
        initial_min_price=125,
        price_step=50,  # This creates ranges like 125-174, 175-224, etc.
        final_min_price_for_open_range=800,  # Last range will be 800 to infinity
        max_pages_per_range=5,  # Lower for testing; Malt allows up to 500.
        # cookie=MALT_COOKIE, # Loaded by default from config.py
        # xsrf_token=MALT_XSRF_TOKEN # Loaded by default from config.py
    )
    scraper.run()