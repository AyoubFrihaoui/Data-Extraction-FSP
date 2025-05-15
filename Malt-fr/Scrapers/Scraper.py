"""
MaltScraper: DataOps-Oriented Web Scraping Pipeline for Malt.fr (v1.2.1)

PURPOSE:
  1) Scrape freelancer profile data (primarily IDs) from Malt.fr based on a search query,
     price ranges, and location. Targets the Malt search API endpoint.
  2) Paginate through search results up to a defined maximum page limit (`max_pages_per_range`)
     for each price segment, implementing an early stopping mechanism if a page returns no results,
     to avoid unnecessary requests.
  3) Fetch pages within a given price range concurrently using Python's `concurrent.futures.ThreadPoolExecutor`
     to speed up the scraping process. The number of concurrent workers is configurable (`num_workers`).
  4) Implement an improved early stopping mechanism using `threading.Event`. This shared event object
     allows the main thread (processing results) to signal all active worker threads immediately
     when a stopping condition (empty page or critical error) is met, minimizing wasted requests.
  5) Showcase a DataOps approach by creating structured metadata files (JSON format) in each
     directory within the `raw_data/` path. These files record run details (parameters, timestamps, duration),
     error states (status, message, stage), and API request context (URL structure, non-sensitive headers),
     facilitating data lineage, debugging, and reproducibility.
  6) Capture all console output (INFO level and above) and detailed debug messages into a dedicated
     log file using Python’s standard `logging` module, providing a comprehensive record of the scraping process.

DIRECTORY STRUCTURE:
  Follows a hierarchical structure to organize scraped data logically:
  raw_data/                 # Root directory for all raw scraped data
    malt_fr/                # Scraper-specific directory (site name)
      metadata_malt_fr.json # Metadata for the site-level directory
      <query_term_slug>/    # Directory named after the URL-safe query term
        metadata_<query_term_slug>.json # Metadata for the query term
        <location_cleaned>/ # Directory named after the cleaned/slugified location
          metadata_<location_cleaned>.json # Metadata for the location
          range_<price_min>_<price_max>/  # Data for a specific price range (e.g., range_125_174)
                                          # or range_<price_min>_inf for the open-ended max range
            page_1.json                   # Raw JSON response for page 1 of this range
            page_2.json                   # Raw JSON response for page 2...
            ...
            metadata_range_<price_min>_<price_max_or_inf>.json # Detailed metadata for this specific price range scrape

LOGGING:
  - A single log file named `scrape_malt_<query_term_slug>.log` is created within the
    innermost `<location_cleaned>` directory for each run.
  - All informational messages (scraper progress, range processing) and debugging details
    (worker activity, errors) are logged. Standard `print` statements are avoided;
    `logger` calls are used instead, directing output to both the console (INFO level)
    and the log file (DEBUG level).

METADATA & ERROR HANDLING:
  - Each directory created by the scraper contains a `metadata_<directory_name>.json` file.
  - These files store status information (`scrape_status`: initialized, processing_started, success,
    error, completed_with_errors), timestamps, the unique `run_id`, and descriptive information.
  - In case of errors during scraping (network issues, HTTP errors, data parsing failures),
    the `scrape_status` is set to "error". Additional fields `error_message` (the error details)
    and `error_stage` (where the error occurred, e.g., which page or range) are populated
    for easier debugging.
  - The metadata for each price range directory (`metadata_range_...json`) includes detailed
    run statistics like duration, number of pages processed, total profiles fetched, the
    reason scraping stopped for that range (`stop_reason`), number of workers used,
    and an `api_request_details` section containing the base URL, an example full URL,
    non-sensitive headers, and the structure of query parameters used.
  - A `runs` dictionary within metadata files tracks details specific to each execution (`run_id`)
    that affected that directory.

'DECEPTION' FIELD:
  - Included in each metadata file, this field provides a plain-language explanation intended
    for future users (human or LLM) about the purpose and structure of the data within that
    specific directory, aiding in data discovery and interpretation.

PRICE RANGES:
  - The scraper iterates through price ranges defined by `initial_min_price`, `price_step`,
    and `final_min_price_for_open_range`. For example, with step 50 and start 125, ranges
    like 125-174, 175-224, etc., are generated.
  - The last range starts at `final_min_price_for_open_range` and has no maximum (`maxPrice`
    parameter is omitted from the API request), effectively searching up to infinity.

DISCLAIMER:
  - This script is provided for educational and illustrative purposes only.
  - Running web scrapers, especially concurrently, can place significant load on target websites.
    Excessive or aggressive scraping can lead to temporary or permanent IP address blocking,
    account suspension, or legal action by the website owner.
  - Always review and respect the website's Terms of Service (robots.txt, usage policies)
    before scraping. Use responsibly and ethically. Adjust `num_workers` and sleep timers
    to minimize impact.

Author: @AyoubFrihaoui
Version: 1.2.1 (Moved sleep to worker, corrected max_page_requested tracking)
"""

# --- Standard Library Imports ---
from datetime import datetime, timezone
import os  # For interacting with the operating system (file paths, directory creation)
import sys  # For system-specific parameters and functions (stdout for logging)
import time  # For pausing execution (time.sleep)
import json  # For working with JSON data (parsing responses, writing metadata)
from typing import Any, Dict, List, Optional, Tuple
import uuid  # For generating unique identifiers (run_id, dynamic request IDs)
import random  # For generating random numbers (sleep durations, potentially user-agent rotation)
import logging  # For logging events, errors, and debug information
import platform  # For getting system/platform information (for metadata)
import re  # For regular expressions (used in slugify)
import threading  # For multi-threading capabilities (Event for early stopping)
from urllib.parse import (
    urlencode,
    quote_plus,
)  # For constructing URL query strings safely

# --- Third-Party Library Imports ---
import requests  # For making HTTP requests to the Malt API

# --- Concurrency Imports ---
from concurrent.futures import (
    ThreadPoolExecutor,  # Manages a pool of worker threads for concurrent execution
    as_completed,  # Iterator that yields futures as they complete
    Future,  # Represents the result of an asynchronous computation
    CancelledError,  # Exception raised when a future is cancelled
)

# --- Configuration Import ---
# Attempt to import sensitive credentials (Cookie, XSRF Token) from a separate 'config.py' file.
# This file should be kept private and not committed to version control (add to .gitignore).
try:
    from config import MALT_COOKIE, MALT_XSRF_TOKEN
except ImportError:
    # Fallback placeholder values if config.py is missing or doesn't contain the variables.
    # The script will likely fail without valid credentials.
    print(
        "ERROR: config.py not found or MALT_COOKIE/MALT_XSRF_TOKEN not set. "
        "Please create config.py with your credentials."
    )
    MALT_COOKIE = "YOUR_MALT_COOKIE_MUST_BE_SET_IN_CONFIG_PY"
    MALT_XSRF_TOKEN = "YOUR_MALT_XSRF_TOKEN_MUST_BE_SET_IN_CONFIG_PY"


# --- Main Scraper Class Definition ---
class MaltScraper:
    """
    Encapsulates the logic for scraping Malt.fr profiles based on search criteria.
    Manages state (session, headers, run ID), directory structure, logging,
    metadata generation, parallel fetching, and error handling.
    """

    # --- Class Constants ---
    # Base URL for the Malt search API endpoint being targeted.
    BASE_API_URL = "https://www.malt.fr/search/api/profiles"

    # Base HTTP headers sent with every request. Some values (like user-agent, sec-ch-ua)
    # should ideally be kept updated to mimic a real browser. Sensitive headers like
    # Cookie and x-xsrf-token are added during initialization from config.
    BASE_HEADERS = {
        "accept": "application/json",  # Indicates we expect a JSON response
        "accept-language": "en-US,en;q=0.9,fr;q=0.8",  # Preferred languages for response content
        "cache-control": "no-cache",  # Ask server and intermediate caches not to serve stale content
        "priority": "u=1, i",  # Request priority hint (browser-specific)
        "referer": "https://www.malt.fr/s",  # A plausible referer URL
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',  # Client hints about browser
        "sec-ch-ua-mobile": "?0",  # Client hints: not a mobile device
        "sec-ch-ua-platform": '"Windows"',  # Client hints: operating system
        "sec-fetch-dest": "empty",  # Request destination context
        "sec-fetch-mode": "cors",  # Request mode
        "sec-fetch-site": "same-origin",  # Request initiator context
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",  # Standard browser User-Agent string
        # Dynamic headers like baggage, sentry-trace, x-app-version, x-xsrf-token are added later
    }

    # --- Initialization Method ---
    def __init__(
        self,
        query_term: str = "developpeur",
        location_name: str = "En télétravail (France)",
        remote_allowed: bool = True,
        initial_min_price: int = 125,
        price_step: int = 50,
        final_min_price_for_open_range: int = 800,
        max_pages_per_range: int = 500,
        num_workers: int = 4,
        cookie: str = MALT_COOKIE,
        xsrf_token: str = MALT_XSRF_TOKEN,
    ):
        """
        Initializes the MaltScraper instance with scraping parameters and sets up state.

        :param query_term: The keyword(s) to search for on Malt (e.g., "python developer").
        :param location_name: The location filter string as used on Malt (e.g., "En télétravail (France)", "Paris", "Lyon").
        :param remote_allowed: Boolean flag to filter for remote-allowed profiles (True/False).
        :param initial_min_price: The starting minimum daily rate (EUR) for the first price range.
        :param price_step: The size of each price range increment (e.g., 50 means ranges 100-149, 150-199...).
        :param final_min_price_for_open_range: The minimum daily rate (EUR) for the last price range, which has no upper bound.
        :param max_pages_per_range: The maximum number of pages (API requests) to attempt fetching for any single price range. Acts as a safeguard. Malt's API typically limits results around page 500.
        :param num_workers: The number of concurrent threads to use for fetching pages within each price range. Higher numbers increase speed but also server load and risk of rate limiting.
        :param cookie: The complete Malt.fr 'Cookie' header string obtained from a logged-in browser session. Essential for authentication.
        :param xsrf_token: The Malt.fr 'X-XSRF-TOKEN' header value, also from a browser session. Required for POST requests and often checked even for GETs.
        """
        # Store configuration parameters
        self.query_term = query_term
        self.query_term_slug = self._slugify(
            query_term
        )  # Create a file-system-safe version of the query
        self.location_name = location_name
        # Create a file-system-safe version of the location name (e.g., "en_teletravail_france")
        self.location_cleaned_slug = self._slugify(location_name.split("(")[0].strip())
        self.remote_allowed_str = str(
            remote_allowed
        ).lower()  # Convert boolean to 'true'/'false' string for URL params
        self.initial_min_price = initial_min_price
        self.price_step = price_step
        self.final_min_price_for_open_range = final_min_price_for_open_range
        self.max_pages_per_range = max_pages_per_range
        self.num_workers = num_workers

        # Initialize a requests.Session object. Using a session allows reusing TCP connections
        # and automatically handles cookies if the site sets them during interaction (though we provide the main one).
        self.session = requests.Session()
        # Copy base headers and add authentication/dynamic headers
        self.headers = self.BASE_HEADERS.copy()
        self.headers["Cookie"] = cookie  # Add the essential authentication cookie
        self.headers["x-xsrf-token"] = xsrf_token  # Add the anti-CSRF token
        # Add other potentially required headers. Values might need updating based on inspecting current browser requests.
        self.headers["x-app-version"] = (
            "unknown_app_version"  # Placeholder, check browser dev tools for current value
        )
        # Update the session object to use these headers for all subsequent requests made by this session.
        self.session.headers.update(self.headers)

        # Generate a unique ID for this specific scraping run. Used for logging and metadata tracking.
        self.run_id = str(uuid.uuid4())

        # Define the base directory path for saving data, structured by site, query, and location.
        self.base_dir = os.path.join(
            "raw_data", "malt_fr", self.query_term_slug, self.location_cleaned_slug
        )
        # Create the directory structure if it doesn't exist. 'exist_ok=True' prevents errors if directories already exist.
        os.makedirs(self.base_dir, exist_ok=True)

        # --- Initialize Infrastructure ---
        self._setup_logging()  # Configure the logger for file and console output.
        self._initialize_directories_and_metadata()  # Create base directories and write initial metadata stubs.

        # --- Log Initialization Completion ---
        self.logger.info(
            f"MaltScraper initialized for query '{self.query_term}' at '{self.location_name}' with {self.num_workers} workers."
        )
        # Check if placeholder credentials are being used and warn the user.
        if MALT_COOKIE.startswith("YOUR_") or MALT_XSRF_TOKEN.startswith("YOUR_"):
            self.logger.warning(
                "Using placeholder Cookie/XSRF-Token. Scraper will likely fail. Update config.py."
            )

    # --- Helper Method: Slugify ---
    def _slugify(self, text: str) -> str:
        """
        Converts a string into a file-system-friendly 'slug'.
        Lowercases, removes non-alphanumeric characters (except hyphens/spaces),
        and replaces hyphens/spaces with underscores.

        :param text: The input string.
        :return: The slugified string.
        """
        text = text.lower()  # Convert to lowercase
        text = re.sub(r"[^\w\s-]", "", text)  # Remove unwanted characters
        text = re.sub(r"[-\s]+", "_", text)  # Replace spaces/hyphens with underscore
        return text.strip("_")  # Remove leading/trailing underscores

    # --- Helper Method: Logging Setup ---
    def _setup_logging(self):
        """Configures the logging system for the scraper instance."""
        # Define the full path for the log file specific to this run's query term.
        log_file = os.path.join(
            self.base_dir, f"scrape_malt_{self.query_term_slug}.log"
        )
        # Get a logger instance named uniquely for this run (helps if running multiple scrapers).
        self.logger = logging.getLogger(f"{self.run_id}_MaltScraper")
        # Set the minimum logging level to capture (DEBUG captures everything).
        self.logger.setLevel(logging.DEBUG)
        # Prevent adding multiple handlers if this method is called again (e.g., during testing).
        if not self.logger.handlers:
            # --- File Handler ---
            # Creates a handler to write logs to the specified file.
            # Mode 'a' appends to the file if it exists. UTF-8 encoding is specified.
            file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            # Set the minimum level for the file handler (DEBUG writes all messages).
            file_handler.setLevel(logging.DEBUG)

            # --- Console Handler ---
            # Creates a handler to write logs to the standard output (console).
            console_handler = logging.StreamHandler(sys.stdout)
            # Set the minimum level for the console handler (INFO shows progress but not detailed debug messages).
            console_handler.setLevel(logging.INFO)

            # --- Formatter ---
            # Defines the format for log messages (timestamp, level, message).
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",  # Timestamp format
            )
            # Apply the formatter to both handlers.
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # --- Add Handlers to Logger ---
            # Attach the configured handlers to the logger instance.
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        # Log that the logger has been initialized.
        self.logger.info(f"Log file initialized at: {log_file}")

    # --- Helper Method: Initialize Directories and Metadata ---
    def _initialize_directories_and_metadata(self):
        """Creates the necessary base directory structure and writes initial metadata files."""
        # Call _init_directory_with_metadata for each level of the defined structure.
        self._init_directory_with_metadata(
            os.path.join("raw_data"), "Root raw_data directory for all scrapers"
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", "malt_fr"),
            "Root directory for Malt.fr scraper data",
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", "malt_fr", self.query_term_slug),
            f"Data for Malt.fr query: '{self.query_term}'",
        )
        self._init_directory_with_metadata(
            self.base_dir,
            f"Data for query '{self.query_term}' at location: '{self.location_name}'",
        )

    # --- Helper Method: Initialize Single Directory Metadata ---
    def _init_directory_with_metadata(self, dir_path: str, description: str) -> None:
        """
        Ensures a directory exists and writes a basic metadata file within it.
        This is part of the DataOps approach to provide context for every data folder.

        :param dir_path: The path to the directory.
        :param description: A brief description of the directory's purpose for the metadata file.
        """
        os.makedirs(dir_path, exist_ok=True)  # Create directory if it doesn't exist
        # Get the base name of the directory for naming the metadata file
        dir_name = os.path.basename(dir_path) or os.path.basename(
            os.path.abspath(dir_path)
        )  # Handle root/relative paths
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")
        # Define the initial metadata structure.
        metadata_stub = {
            "deception": "Metadata for understanding directory content and run context.",  # Explanation for users/LLMs
            "directory_name": dir_name,  # Name of the directory this file describes
            "creation_time": datetime.now(timezone.utc).isoformat()
            + "Z",  # Timestamp of initial creation (UTC)
            "scrape_status": "initialized",  # Initial status
            "run_id": self.run_id,  # Link to the specific run that created it
            "description": description,  # Purpose of this directory
        }
        # Write the initial metadata file. Overwrites if it exists, which is acceptable for initialization.
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(
                metadata_stub, f, ensure_ascii=False, indent=2
            )  # Pretty-print JSON

    # --- Helper Method: Update Directory Metadata ---
    def _update_directory_metadata(
        self,
        dir_path: str,
        scrape_status: str,
        error_message: Optional[str] = None,
        error_stage: Optional[str] = None,
        extra_data: Optional[dict] = None,
    ) -> None:
        """
        Updates the metadata file in a given directory with new status, error info, and run-specific details.
        Loads existing metadata if present, updates fields, and writes back.

        :param dir_path: Path to the directory containing the metadata file.
        :param scrape_status: The new status string (e.g., "processing_started", "success", "error").
        :param error_message: Optional error message string if status is "error".
        :param error_stage: Optional string indicating where the error occurred if status is "error".
        :param extra_data: Optional dictionary containing additional run-specific details to merge into the metadata.
        """
        dir_name = os.path.basename(dir_path) or os.path.basename(
            os.path.abspath(dir_path)
        )
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")
        try:
            # Attempt to load existing metadata.
            if os.path.isfile(metadata_file):
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            else:
                # If metadata file is somehow missing, recreate a basic structure.
                self.logger.warning(
                    f"Metadata file missing at {metadata_file}, recreating."
                )
                metadata = {
                    "deception": "Metadata for understanding directory content and run context.",
                    "directory_name": dir_name,
                    "creation_time": datetime.now(timezone.utc).isoformat()
                    + "Z",  # Set creation time to now
                    "run_id": self.run_id,  # Associate with current run
                }

            # --- Update Top-Level Metadata ---
            # These fields reflect the status of the *last* run that modified this directory.
            metadata["scrape_status"] = scrape_status
            metadata["last_updated_time"] = (
                datetime.now(timezone.utc).isoformat() + "Z"
            )  # Timestamp of this update
            # Add error details if provided.
            if error_message:
                metadata["error_message"] = error_message
            if error_stage:
                metadata["error_stage"] = error_stage

            # --- Update Run-Specific Metadata ---
            # Ensure the 'runs' dictionary exists to store details per run_id.
            if "runs" not in metadata:
                metadata["runs"] = {}
            # Ensure an entry for the current run_id exists within 'runs'.
            if self.run_id not in metadata["runs"]:
                metadata["runs"][self.run_id] = {}

            # Get the dictionary for the current run's details.
            run_details = metadata["runs"][self.run_id]
            # Update the status and error info specifically for this run.
            run_details["scrape_status"] = scrape_status
            if error_message:
                run_details["error_message"] = error_message
            if error_stage:
                run_details["error_stage"] = error_stage
            # If extra data (like run parameters, stats) is provided, add it to this run's details.
            if extra_data:
                run_details.update(extra_data)
                # Optionally, update selected top-level keys as well (e.g., overall duration, last stop reason).
                # This provides a quick summary without needing to dive into the specific run.
                for k, v in extra_data.items():
                    if k in [
                        "duration_seconds",
                        "stop_reason",
                        "pages_processed_in_range",
                        "total_profiles_in_range",
                        "max_page_requested",
                    ]:
                        metadata[k] = v

            # --- Write Updated Metadata ---
            # Write the modified metadata dictionary back to the JSON file.
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)  # Pretty-print

        except Exception as e:
            # Log an error if metadata update fails, but don't stop the scraper.
            self.logger.error(f"Failed to update metadata file at {metadata_file}: {e}")

    # --- Helper Method: Generate Price Ranges ---
    def _generate_price_ranges(self) -> List[Tuple[int, Optional[int]]]:
        """
        Generates a list of price range tuples based on the scraper's configuration.
        Each tuple is (min_price, max_price). The last range has max_price=None.

        :return: A list of tuples, e.g., [(125, 174), (175, 224), ..., (800, None)].
        """
        ranges: List[Tuple[int, Optional[int]]] = []
        current_min = self.initial_min_price
        # Loop while the current minimum is less than the start of the final open range.
        while current_min < self.final_min_price_for_open_range:
            # Calculate the maximum for this step (inclusive).
            current_max = current_min + self.price_step - 1
            ranges.append((current_min, current_max))
            # Increment the minimum for the next step.
            current_min += self.price_step
        # Add the final open-ended range (min_price, None means no maxPrice in API).
        ranges.append((self.final_min_price_for_open_range, None))
        self.logger.info(f"Generated {len(ranges)} price ranges to scrape.")
        return ranges

    # --- Helper Method: Build API URL ---
    def _build_api_url(
        self, page: int, price_min: int, price_max: Optional[int]
    ) -> str:
        """
        Constructs the specific Malt API URL for a given page and price range.

        :param page: The page number to request (parameter 'p').
        :param price_min: The minimum price for the range (parameter 'minPrice').
        :param price_max: The maximum price for the range, or None for the open-ended range (parameter 'maxPrice').
        :return: The fully constructed API URL string with query parameters.
        """
        # Base parameters required for the search.
        params: Dict[str, Any] = {
            "q": self.query_term,  # Search keyword
            "remoteAllowed": self.remote_allowed_str,  # 'true' or 'false'
            "location": self.location_name,  # Location string (Malt expects the display name)
            "p": page,  # Page number
            "minPrice": price_min,  # Minimum daily rate
            "searchid": uuid.uuid4().hex[
                :24
            ],  # Generate a plausible dynamic search ID per request
        }
        # Add maxPrice parameter only if it's not the open-ended range.
        if price_max is not None:
            params["maxPrice"] = price_max
        # Use urlencode with quote_plus to correctly format the query string part of the URL.
        return f"{self.BASE_API_URL}?{urlencode(params, quote_via=quote_plus)}"

    # --- Worker Function: Fetch Single Page ---
    def _fetch_single_page(
        self,
        page_num: int,
        price_min: int,
        price_max: Optional[int],
        range_dir: str,
        stop_event: threading.Event,
    ) -> Tuple[int, Optional[dict]]:
        """
        Fetches and saves data for a single page. Designed to be run in a worker thread.
        Checks the shared `stop_event` before and after the network request.

        :param page_num: The page number to fetch.
        :param price_min: The minimum price for the current range context.
        :param price_max: The maximum price for the current range context (or None).
        :param range_dir: The directory path where the page's JSON data should be saved.
        :param stop_event: The shared threading.Event object to check for early stopping signals.
        :return: A tuple containing (page_number, parsed_json_data). parsed_json_data is None if
                 an error occurred or if the stop_event was set.
        """
        # --- Pre-Request Stop Check ---
        # Immediately check if the stop signal has been set by another thread.
        if stop_event.is_set():
            self.logger.debug(
                f"Worker stopping pre-fetch for page {page_num} due to stop event."
            )
            return (
                page_num,
                None,
            )  # Return None to indicate no data was fetched/processed

        # Construct the URL for this specific page request.
        request_url = self._build_api_url(page_num, price_min, price_max)
        # Define the file path where the result will be saved.
        page_file = os.path.join(range_dir, f"page_{page_num}.json")
        response = (
            None  # Initialize response variable for potential use in error handling
        )

        try:
            # --- Prepare Request Headers ---
            # Create dynamic headers for this specific request, potentially helping avoid detection.
            dynamic_headers = (
                self.session.headers.copy()
            )  # Start with session headers (incl. Cookie, XSRF)
            # Add/update headers that might change per request (e.g., tracing IDs).
            dynamic_headers["baggage"] = (
                f"sentry-environment=production,sentry-public_key=f56...,sentry-trace_id={uuid.uuid4().hex}"  # Example Sentry baggage
            )
            dynamic_headers["sentry-trace"] = (
                f"{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}"  # Example Sentry trace ID
            )

            # --- Make Network Request ---
            self.logger.debug(f"Worker fetching page {page_num} from {request_url}")
            # Perform the GET request using the shared session, applying dynamic headers and a timeout.
            response = self.session.get(
                request_url, headers=dynamic_headers, timeout=45
            )  # Increased timeout

            # --- Post-Request Stop Check ---
            # Check the stop signal again immediately after the request returns, before processing.
            # This handles cases where the stop was signaled while the request was in flight.
            if stop_event.is_set():
                self.logger.debug(
                    f"Worker stopping post-fetch for page {page_num} due to stop event."
                )
                return page_num, None  # Return None, do not process/save the result

            # --- Process Successful Response ---
            # Check for HTTP errors (4xx or 5xx status codes). Raises requests.exceptions.HTTPError if found.
            response.raise_for_status()
            # Parse the JSON response content into a Python dictionary. Raises json.JSONDecodeError on failure.
            data = response.json()

            # --- Save Data ---
            # Write the successfully parsed JSON data to its designated file.
            with open(page_file, "w", encoding="utf-8") as f:
                json.dump(
                    data, f, ensure_ascii=False, indent=2
                )  # Save pretty-printed JSON
            self.logger.debug(f"  - Saved page {page_num} data to {page_file}")

            # --- Polite Sleep ---
            # Introduce a short, random delay after successfully processing a page.
            # This helps pace the requests made by this *individual worker*.
            sleep_duration = round(
                random.uniform(0.25, 0.45), 4
            )  # Adjust timing as needed
            time.sleep(sleep_duration)

            # Return the page number and the fetched data.
            return page_num, data

        # --- Error Handling ---
        except requests.exceptions.Timeout:
            # Log timeouts as warnings, as they might be temporary network issues.
            self.logger.warning(
                f"Timeout fetching page {page_num} for range {price_min}-{price_max}."
            )
            return page_num, None  # Indicate failure for this page
        except requests.exceptions.HTTPError as http_err:
            # Log HTTP errors. Log 404s (Not Found) or 403s (Forbidden) as warnings, others as errors.
            log_level = (
                logging.ERROR
                if http_err.response.status_code not in [404, 403]
                else logging.WARNING
            )
            self.logger.log(
                log_level,
                f"HTTP error {http_err.response.status_code} fetching page {page_num}: {http_err}",
            )
            # Specifically log a critical error for 403, suggesting common causes.
            if http_err.response.status_code == 403:
                self.logger.error(
                    "Received 403 Forbidden. Check Cookie/Token validity, IP reputation, or potential rate limiting."
                )
            return page_num, None  # Indicate failure
        except json.JSONDecodeError as json_err:
            # Handle errors parsing the response as JSON (e.g., server returned HTML error page).
            self.logger.error(f"JSON decode error on page {page_num}: {json_err}.")
            # Attempt to save the raw response text for debugging purposes.
            if response is not None:  # Check if response object exists
                bad_response_file = page_file.replace(".json", ".bad_response.txt")
                try:
                    with open(bad_response_file, "w", encoding="utf-8") as f_err:
                        f_err.write(response.text)  # Save the raw text content
                    self.logger.error(
                        f"Bad response content saved to {bad_response_file}"
                    )
                except Exception as save_err:
                    # Log error if saving the bad response fails too.
                    self.logger.error(
                        f"Attempted to save bad response, but failed: {save_err}"
                    )
            return page_num, None  # Indicate failure
        except Exception as e_req:
            # Catch any other unexpected exceptions during the request/processing.
            # Avoid logging noisy errors if the stop event was already set (e.g., connection aborted).
            if not stop_event.is_set():
                self.logger.error(
                    f"Generic error occurred while fetching page {page_num}: {e_req}",
                    exc_info=True,
                )  # Include traceback
            else:
                # Log as debug if likely caused by the stop signal.
                self.logger.debug(
                    f"Generic error likely due to stop signal for page {page_num}: {e_req}"
                )
            return page_num, None  # Indicate failure/stop

    # --- Range Processing Function ---
    def _fetch_profiles_for_range(self, price_min: int, price_max: Optional[int]):
        """
        Manages the parallel fetching of pages for a single price range.
        Uses ThreadPoolExecutor and a shared stop_event for efficient early stopping.

        :param price_min: Minimum price for this range.
        :param price_max: Maximum price for this range (or None).
        :return: Boolean indicating if the range processing completed without critical errors (True) or failed (False).
                 Note: A clean early stop is considered success.
        """
        # --- Setup for the Range ---
        range_label = (
            f"{price_min}_{price_max}" if price_max is not None else f"{price_min}_inf"
        )
        range_dir_name = f"range_{range_label}"
        range_dir = os.path.join(self.base_dir, range_dir_name)
        os.makedirs(range_dir, exist_ok=True)  # Ensure range directory exists
        # Initialize metadata specifically for this range directory.
        self._init_directory_with_metadata(
            range_dir,
            f"Data for price range: {price_min} EUR to {price_max if price_max else 'infinity'} EUR",
        )

        # --- State Variables for Range Processing ---
        start_time = time.time()  # Record start time for duration calculation
        total_profiles_in_range = 0  # Accumulator for profiles found in this range
        processed_page_numbers = (
            set()
        )  # Keep track of page numbers successfully fetched *and* saved
        max_page_requested = (
            0  # Track the highest page number *submitted* to the executor
        )
        scrape_status = "success"  # Assume success initially for this range
        error_message = None  # Placeholder for error details
        error_stage = None  # Placeholder for error location context
        stop_reason = "completed"  # Reason why processing stopped (updated later)
        futures_map: Dict[Future, int] = (
            {}
        )  # Dictionary to map Future objects back to their page numbers
        stop_event = (
            threading.Event()
        )  # Create a *new* stop event for *this specific range*
        first_empty_page_found = float(
            "inf"
        )  # Track the page number of the first empty page found (after page 1)

        self.logger.info(
            f"Fetching data for price range [{range_label}] using {self.num_workers} workers..."
        )
        last_submitted_page = (
            0  # Track the page number of the last task actually submitted
        )

        try:
            # --- Thread Pool Execution ---
            # Create a thread pool context manager. Threads are automatically joined on exit.
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                # --- Submit Tasks ---
                # Loop to submit fetching tasks for pages 1 up to the configured maximum.
                for page_num in range(1, self.max_pages_per_range + 1):
                    # Check the stop event *before* submitting a new task.
                    if stop_event.is_set():
                        self.logger.debug(
                            f"Stop event set, not submitting further pages starting from {page_num}. Last submitted: {last_submitted_page}"
                        )
                        break  # Exit the submission loop
                    # Submit the task to the executor: call _fetch_single_page with necessary args including stop_event.
                    future = executor.submit(
                        self._fetch_single_page,
                        page_num,
                        price_min,
                        price_max,
                        range_dir,
                        stop_event,
                    )
                    # Map the returned Future object to its corresponding page number for later retrieval.
                    futures_map[future] = page_num
                    last_submitted_page = (
                        page_num  # Record the page number just submitted
                    )

                # Record the highest page number that was actually submitted to the executor.
                max_page_requested = last_submitted_page

                # --- Process Completed Tasks ---
                # Use as_completed to process results as soon as workers finish their tasks (order is not guaranteed).
                for future in as_completed(futures_map):
                    page_num = futures_map[
                        future
                    ]  # Get the page number associated with this completed future
                    try:
                        # Retrieve the result from the future. This blocks until the future is done.
                        # result() returns the tuple (page_num, data_or_none) from _fetch_single_page.
                        _, page_data = future.result()

                        # --- Handle Worker Results ---
                        if page_data is None:
                            # Worker returned None. This means either:
                            # 1. An error occurred within the worker (_fetch_single_page handled logging).
                            # 2. The worker stopped cleanly because stop_event was set.
                            # Only flag the range scrape status as "error" if the stop event wasn't *already* set
                            # and if the overall status is still "success" (avoid overwriting previous errors).
                            if not stop_event.is_set() and scrape_status == "success":
                                scrape_status = "error"
                                error_message = f"Worker failed or stopped unexpectedly for page {page_num}."
                                error_stage = (
                                    f"{range_dir_name}_page_{page_num}_fetch_error"
                                )
                                stop_reason = "error"
                                self.logger.error(
                                    f"Error detected processing page {page_num}. Signaling stop for range {range_label}."
                                )
                                stop_event.set()  # Signal other workers to stop due to this error
                            # If stopped cleanly or error occurred after stop signal, just continue to next future.
                            continue  # Skip further processing for this page

                        # --- Process Valid Data ---
                        # If page_data is not None, the fetch and save were successful.
                        processed_page_numbers.add(
                            page_num
                        )  # Add to set of successfully processed pages
                        # Extract profiles from the data (handle potential missing key gracefully).
                        profiles = page_data.get("profiles", [])
                        num_profiles_on_page = len(profiles)
                        # Accumulate the total profile count for this range.
                        total_profiles_in_range += num_profiles_on_page

                        self.logger.info(
                            f"  - Worker finished page {page_num}: Found {num_profiles_on_page} profiles."
                        )

                        # --- Early Stopping Logic ---
                        # Check if the page (after page 1) is empty.
                        if num_profiles_on_page == 0 and page_num > 1:
                            # Check if this is the *first* time an empty page is encountered for this range.
                            if page_num < first_empty_page_found:
                                first_empty_page_found = page_num  # Record the number of this first empty page
                                self.logger.info(
                                    f"  - Page {page_num} is first empty page found. Signaling stop for this range."
                                )
                                # Set the shared stop event if it hasn't been set already.
                                if not stop_event.is_set():
                                    stop_event.set()
                                    stop_reason = "stopped_empty"  # Update the reason for stopping

                    # --- Handle Future Exceptions ---
                    except CancelledError:
                        # Log if a future was explicitly cancelled (likely due to stop_event).
                        self.logger.debug(f"Future for page {page_num} was cancelled.")
                    except Exception as e_future:
                        # Catch any other exceptions that occurred when calling future.result().
                        self.logger.error(
                            f"Error processing result for page {page_num}: {e_future}",
                            exc_info=True,
                        )
                        # Update scrape status to error if it's not already set.
                        if scrape_status == "success":
                            scrape_status = "error"
                            error_message = f"Failed during result processing for page {page_num}: {e_future}"
                            error_stage = (
                                f"{range_dir_name}_page_{page_num}_result_processing"
                            )
                            stop_reason = "error"
                            # Signal stop if not already signaled.
                            if not stop_event.is_set():
                                stop_event.set()

            # --- Post-Loop Cleanup / Status Refinement ---
            # Determine the highest page number that was successfully processed and saved.
            highest_processed_page = (
                max(processed_page_numbers) if processed_page_numbers else 0
            )
            # Refine the stop_reason if the loop completed without error or early stop signal.
            if stop_reason == "completed":
                # If the highest page submitted reached the limit, that's the reason.
                if max_page_requested >= self.max_pages_per_range:
                    stop_reason = "max_pages"
                # If the stop event was set, but the reason is still 'completed', refine it.
                elif stop_event.is_set():
                    stop_reason = (
                        "stopped_early"  # Generic stop if not empty/error/max_pages
                    )

        # --- Outer Exception Handling ---
        except Exception as e_outer:
            # Catch exceptions related to the ThreadPoolExecutor setup or shutdown.
            scrape_status = "error"
            error_message = f"Outer exception occurred during parallel processing for range {range_label}: {e_outer}"
            error_stage = f"{range_dir_name}_executor_management"
            stop_reason = "error"
            self.logger.error(error_message, exc_info=True)  # Log with traceback
            # Ensure stop is signaled if an outer error occurs.
            if not stop_event.is_set():
                stop_event.set()

        # --- Final Calculations and Logging for Range ---
        end_time = time.time()  # Record end time
        duration_seconds = round(end_time - start_time, 2)  # Calculate duration

        # Calculate the final count of pages *successfully processed before the first empty page*.
        final_processed_page_count = 0
        if processed_page_numbers:  # Check if any pages were processed at all
            if first_empty_page_found != float("inf"):
                # If an empty page was found, count only successfully processed pages *before* it.
                final_processed_page_count = len(
                    [p for p in processed_page_numbers if p < first_empty_page_found]
                )
            else:
                # If no empty page was found, count all successfully processed pages.
                final_processed_page_count = len(processed_page_numbers)

        # Log summary for this price range.
        self.logger.info(
            f"Finished range [{range_label}] with status='{scrape_status}'. "
            f"Stop reason: {stop_reason}. Pages processed successfully (before stop): {final_processed_page_count}. "
            f"Highest page submitted: {max_page_requested}. Total profiles fetched (all processed pages): {total_profiles_in_range}."
        )

        # --- Prepare and Update Range Metadata ---
        # Collect non-sensitive headers for metadata.
        safe_headers = {
            k: v
            for k, v in self.session.headers.items()
            if k.lower() not in ["cookie", "x-xsrf-token"]
        }
        # Gather scraper environment info.
        scraper_info = {
            "python_version": sys.version.split("\n")[0],
            "platform": platform.platform(),
            "scraper_version": "1.2.1",
        }
        # Get an example URL for the first page of this range.
        first_page_url_for_metadata = self._build_api_url(1, price_min, price_max)
        # Compile all extra metadata specific to this range's execution.
        extra_meta = {
            "start_time_iso": datetime.fromtimestamp(start_time, timezone.utc).isoformat() + "Z",
            "end_time_iso": datetime.fromtimestamp(end_time, timezone.utc).isoformat() + "Z",
            "duration_seconds": duration_seconds,
            "pages_processed_in_range": final_processed_page_count,  # The count before stopping
            "max_page_requested": max_page_requested,  # Highest page number submitted
            "first_empty_page_found": (
                first_empty_page_found
                if first_empty_page_found != float("inf")
                else None
            ),  # Page num or null
            "total_profiles_in_range": total_profiles_in_range,  # Count from all pages processed
            "stop_reason": stop_reason,  # Why processing stopped for this range
            "num_workers_used": self.num_workers,  # Number of workers configured
            "api_request_details": {
                "base_url": self.BASE_API_URL,
                "example_full_url_first_page": first_page_url_for_metadata,
                "headers_sent (excluding sensitive)": safe_headers,
                "query_parameters_structure": {  # Document API parameters used
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
            "scraper_environment": scraper_info,
        }
        # Update the metadata file for this specific range directory.
        self._update_directory_metadata(
            dir_path=range_dir,
            scrape_status=scrape_status,
            error_message=error_message,
            error_stage=error_stage,
            extra_data=extra_meta,
        )
        # Return True if the range completed without critical errors (scrape_status is not 'error').
        # A clean stop (e.g., 'stopped_empty', 'max_pages') is considered successful completion of the range task.
        return scrape_status != "error"

    # --- Main Execution Method ---
    def run(self):
        """
        The main entry point to start the scraping process.
        It orchestrates the scraping across all configured price ranges.
        """
        if MALT_COOKIE.startswith("YOUR_") or MALT_XSRF_TOKEN.startswith("YOUR_"):
            print(
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            )
            print(
                "!!! WARNING: MALT_COOKIE or MALT_XSRF_TOKEN in config.py is not set.    !!!"
            )
            print(
                "!!!          The scraper will likely fail due to authentication error.  !!!"
            )
            print(
                "!!!          Please update config.py with your valid credentials.       !!!"
            )
            print(
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            )

        # --- Initial Logging and Metadata ---
        # Log the start of the entire scraping run with key parameters.
        self.logger.info(
            f"Starting Malt.fr scraping run ID: {self.run_id}\n"
            f"Query: '{self.query_term}', Location: '{self.location_name}'\n"
            f"Price Config: Start {self.initial_min_price}, Step {self.price_step}, Open Range From {self.final_min_price_for_open_range}\n"
            f"Max Pages per Range: {self.max_pages_per_range}, Workers per Range: {self.num_workers}"
        )
        # Prepare run parameters for metadata logging, excluding non-serializable objects like session/logger.
        run_params_to_log = {
            k: v
            for k, v in self.__dict__.items()
            if k
            not in [
                "session",
                "logger",
                "headers",
                "query_term_slug",
                "location_cleaned_slug",
                "remote_allowed_str",
            ]  # Exclude internal/sensitive/redundant
        }
        run_params_to_log["query_term"] = (
            self.query_term
        )  # Ensure key params are included
        run_params_to_log["location_name"] = self.location_name
        run_params_to_log["remote_allowed"] = self.remote_allowed_str == "true"

        # Update the metadata for the base directory (query/location level) to show processing has started.
        self._update_directory_metadata(
            self.base_dir,
            scrape_status="processing_started",
            extra_data={
                "run_parameters": run_params_to_log
            },  # Add run parameters to metadata
        )

        # --- Price Range Iteration ---
        overall_success = (
            True  # Flag to track if *any* range encountered critical errors.
        )
        try:
            # Generate the list of price ranges to scrape.
            price_ranges = self._generate_price_ranges()

            # Iterate through each price range sequentially.
            for i, (p_min, p_max) in enumerate(price_ranges):
                # Construct a label for logging/directory naming.
                range_label = (
                    f"{p_min}_{p_max}" if p_max is not None else f"{p_min}_inf"
                )
                self.logger.info(
                    f"--- Processing Price Range {i+1}/{len(price_ranges)}: [{range_label}] ---"
                )

                # Call the method to fetch profiles for this specific range. Parallelism happens *within* this call.
                range_success = self._fetch_profiles_for_range(p_min, p_max)

                # If any range fails critically (returns False), mark the overall run as having errors.
                if not range_success:
                    overall_success = False
                    self.logger.warning(
                        f"Price range [{range_label}] encountered critical errors."
                    )
                    # NOTE: The loop continues to the next range even if one fails.

                # --- Inter-Range Delay ---
                # Introduce a delay between processing different price ranges.
                # This is crucial for politeness and reducing sustained load on the server.
                if i < len(price_ranges) - 1:  # Don't sleep after the last range
                    # Use a slightly longer delay if the previous range had issues.
                    delay_factor = 1 if range_success else 1.5
                    inter_range_delay = round(
                        random.uniform(2 * delay_factor, 5 * delay_factor), 1
                    )  # Adjust base delay as needed
                    self.logger.info(
                        f"Sleeping for {inter_range_delay}s before next price range..."
                    )
                    time.sleep(inter_range_delay)

            # --- Final Status Update ---
            # Determine the final overall status based on whether any range failed.
            final_status = "success" if overall_success else "completed_with_errors"
            # Update the base directory's metadata with the final overall status.
            self._update_directory_metadata(self.base_dir, scrape_status=final_status)
            self.logger.info(
                f"Malt.fr scraping run {self.run_id} finished with overall status: {final_status}!"
            )

        # --- Main Execution Error Handling ---
        except Exception as e_main:
            # Catch any unexpected errors during the main run orchestration (e.g., range generation).
            self.logger.error(
                f"Critical error in main run execution: {e_main}", exc_info=True
            )  # Log with traceback
            # Update the base directory's metadata to indicate a fatal error in the run.
            self._update_directory_metadata(
                self.base_dir,
                scrape_status="error",
                error_message=f"Critical failure in run execution: {str(e_main)}",
                error_stage="main_run_orchestration",
            )
            # Depending on how the script is used, you might want to re-raise the exception:
            # raise


# ------------------------------------------------------------------------------
# EXAMPLE USAGE BLOCK
# This code runs only when the script is executed directly (not imported as a module).
# ------------------------------------------------------------------------------
if __name__ == "__main__":

    # --- Configuration Check ---
    # Remind the user to configure credentials if placeholder values are detected.
    if MALT_COOKIE.startswith("YOUR_") or MALT_XSRF_TOKEN.startswith("YOUR_"):
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        print(
            "!!! WARNING: MALT_COOKIE or MALT_XSRF_TOKEN in config.py is not set.    !!!"
        )
        print(
            "!!!          The scraper will likely fail due to authentication error.  !!!"
        )
        print(
            "!!!          Please update config.py with your valid credentials.       !!!"
        )
        print(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )
        # Optionally, exit if configuration is absolutely required:
        # sys.exit("Configuration error: Malt credentials not set in config.py.")

    # --- Scraper Instantiation and Configuration ---
    # Create an instance of the MaltScraper class with desired parameters.
    # Adjust these values for your specific scraping target.
    scraper = MaltScraper(
        query_term="data analyst freelance",  # Target keyword/role
        location_name="En télétravail (France)",  # Target location
        remote_allowed=True,  # Filter for remote?
        initial_min_price=125,  # Starting price for ranges
        price_step=50,  # Size of price steps
        final_min_price_for_open_range=900,  # Start of the 'infinity' range
        max_pages_per_range=50,  # Limit pages per range (e.g., for testing or politeness)
        # Set higher (e.g., 500) to test stopping logic thoroughly.
        num_workers=6,  # Number of parallel download threads per range. Adjust carefully.
        # cookie=MALT_COOKIE,                     # Loaded by default from config.py
        # xsrf_token=MALT_XSRF_TOKEN              # Loaded by default from config.py
    )

    # --- Run the Scraper ---
    # Call the main 'run' method to start the scraping process.
    scraper.run()

    # --- Completion Message ---
    print("\n--- Scraping script execution finished ---")
