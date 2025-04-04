# src/yoopies_scraper.py  (or wherever you place the main scraper class)

import os
import sys
import time
import json
import uuid
import random
import logging
import platform
import requests
from datetime import datetime
from typing import Optional, Dict, Any

# Append parent directory if needed to import config or utils
# sys.path.append('..') 

# --- Configuration ---
# Option 1: Import from config.py (Recommended)
try:
    from config import YOOPIES_COOKIE, YOOPIES_GRAPHQL_URL
except ImportError:
    print("Warning: config.py not found or YOOPIES_COOKIE/YOOPIES_GRAPHQL_URL not defined.")
    print("Using placeholder values. Ensure COOKIE is set correctly.")
    YOOPIES_COOKIE = "YOUR_YOOPIES_COOKIE_STRING" # Replace with your actual cookie
    YOOPIES_GRAPHQL_URL = "https://yoopies.fr/graphql"

# Option 2: Set directly (Less secure for cookies)
# YOOPIES_COOKIE = "YOUR_YOOPIES_COOKIE_STRING"
# YOOPIES_GRAPHQL_URL = "https://yoopies.fr/graphql"
# ---------------------


class YoopiesScraper:
    """
    A DataOps-oriented scraper for Yoopies.fr (specifically France) childcare data,
    using its GraphQL endpoint.

    Features:
      - Follows a defined directory structure for raw data and metadata.
      - Generates detailed metadata JSON files for traceability and context.
      - Implements robust logging to console and file.
      - Handles pagination using the GraphQL endpoint's cursor mechanism.
      - Designed for childcare ('garde d'enfants') / babysitting ('baby-sitting').

    Directory Structure:
      data/raw/yoopies/
        metadata_yoopies.json  (Root metadata for yoopies)
        <postal_code>/
          metadata_<postal_code>.json
          <care_type>/             # e.g., childcare
            metadata_<care_type>.json
            <sub_type>/           # e.g., babysitting
              metadata_<sub_type>.json
              page_1.json
              page_2.json
              ...
              # No individual metadata per page, aggregate metadata is in metadata_<sub_type>.json

    Logging:
      - A log file (e.g., scrape_babysitting.log) is placed in the <sub_type> directory.
    """

    # --- Constants ---
    GRAPHQL_URL = YOOPIES_GRAPHQL_URL

    # Base Headers (Cookie will be added dynamically) - Adapted from your example
    BASE_HEADERS = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,fr;q=0.8', # Added French
        'baggage': 'sentry-environment=production,sentry-release=6deedcdc89eb92b34f1b1d1271166d967715b4f0,sentry-public_key=b665305b897af0236397145fffb5db66,sentry-trace_id=0cbfc623560a424ebdc8152093164b9a,sentry-sample_rate=0.1', # Example baggage
        'content-type': 'application/json',
        'fromhost': 'yoopies.fr',
        'origin': 'https://yoopies.fr',
        'priority': 'u=1, i',
        'referer': 'https://yoopies.fr/recherche-baby-sitting/results', # Generic referer
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sentry-trace': '0cbfc623560a424ebdc8152093164b9a-b97e745cc2d35f53-0', # Example trace
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }

    # Yoopies GraphQL Query (getAds) - Adapted from your example
    QUERY = """
    query getAds($after: String, $filters: AdsFiltersInput!, $sortBy: AdSearchSortByEnum, $locale: String, $currentUserId: Int, $service: ServiceEnum, $page: Int, $first: Int!) {
      ads(
        first: $first
        after: $after
        filters: $filters
        sortBy: $sortBy
        locale: $locale
        page: $page
      ) {
        count
        countPages
        perPage
        currentPage
        distance
        pageInfo {
          hasNextPage
          hasPreviousPage
          endCursor
          __typename
        }
        edges {
          node {
            score
            ad {
              id
              slug {
                title
                city
                __typename
              }
              title
              content
              type
              status
              createdAt
              updatedAt
              categories {
                service
                category
                __typename
              }
              ... on ApplicantAd {
                experienceAmount
                badges {
                  name
                  additionalData
                  __typename
                }
                employmentTypes {
                  id
                  careTypes {
                    id
                    rateTypes {
                      id
                      rate {
                        amount
                        timeUnit
                        commission
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                tags {
                  primaryService
                  primaryCategory
                  canDoService
                  canDoCategory
                  canDoTypes
                  extraService
                  languages {
                    code
                    __typename
                  }
                  workingConditions {
                    shareRoom
                    workWithCoworker
                    __typename
                  }
                  apartmentDetails {
                    houseType
                    houseSurface
                    gardenType
                    __typename
                  }
                  __typename
                }
                __typename
              }
              ... on SitterAd {
                parentAdType
                childminderSlots {
                  occupiedSlots {
                    available
                    estimatedAvailabilityDate
                    ageRange
                    careTypes
                    __typename
                  }
                  maxAge
                  __typename
                }
                __typename
              }
              address {
                latitude
                longitude
                city
                cityKey
                country
                zipCode
                __typename
              }
              user {
                id
                enabled
                baseType
                roles
                photos {
                  thumbnail
                  large
                  extra_large
                  __typename
                }
                isVerified
                firstName
                lastName
                threadsConnections(first: 1, filters: {user: $currentUserId, service: $service}) {
                  edges {
                    node {
                      id
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                ... on Applicant {
                  hasAEStatus
                  age
                  grades {
                    count
                    average
                    edges {
                      node {
                        owner {
                          firstName
                          lastName
                          __typename
                        }
                        content
                        generalGrade
                        experienceName
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  options {
                    visaExpiryDate
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }
    """
    # --- Initialization ---
    def __init__(
        self,
        postal_code: str,
        city: str,
        latitude: float,
        longitude: float,
        country_name: str = "yoopies", # Using 'yoopies' as the root dir name
        country_code: str = "FR",
        care_type: str = "childcare",   # Maps to 'service' in GraphQL vars
        sub_type: str = "babysitting",  # Maps to 'type' filter in GraphQL vars
        search_page_size: int = 100,    # Yoopies seems to return up to 100
        search_radius_km: int = 50,     # Default distance filter
        locale: str = "fr_FR",          # Locale for the query
        cookie: str = YOOPIES_COOKIE
    ):
        """
        Initializes the YoopiesScraper.

        :param postal_code: The postal code to search within (e.g., "75000").
        :param city: The city name matching the postal code (e.g., "Paris").
        :param latitude: Latitude for the search center.
        :param longitude: Longitude for the search center.
        :param country_name: Root directory name (defaults to 'yoopies').
        :param country_code: Country code for filters (defaults to 'FR').
        :param care_type: High-level service type (e.g., 'childcare'). Used for directory structure and 'service' variable.
        :param sub_type: Specific service sub-type (e.g., 'babysitting'). Used for directory structure and 'type' filter.
        :param search_page_size: Number of results per GraphQL request (max ~100).
        :param search_radius_km: Search radius in kilometers.
        :param locale: Locale string for the GraphQL query.
        :param cookie: The authentication cookie string for yoopies.fr.
        """
        if not cookie or "YOUR_YOOPIES_COOKIE_STRING" in cookie:
            raise ValueError("Yoopies Cookie is not set or is using the placeholder value.")
            
        self.postal_code = postal_code
        self.city = city
        self.latitude = latitude
        self.longitude = longitude
        self.country_name = country_name
        self.country_code = country_code
        self.care_type = care_type.lower()
        self.sub_type = sub_type.lower()
        self.search_page_size = search_page_size
        self.search_radius_km = search_radius_km
        self.locale = locale
        self.cookie = cookie

        self.run_id = str(uuid.uuid4()) # Unique ID for this scraping run

        # --- Directory Structure Setup ---
        # data/raw/yoopies/<postal_code>/<care_type>/<sub_type>/
        self.base_dir = os.path.join(
            "data", "raw", self.country_name, self.postal_code, self.care_type, self.sub_type
        )
        os.makedirs(self.base_dir, exist_ok=True)

        # --- Session Setup ---
        self.session = requests.Session()
        self.session.headers.update(self.BASE_HEADERS)
        self.session.headers["Cookie"] = self.cookie # Add the specific cookie

        # --- Logging Setup ---
        self._setup_logging() # Sets up self.logger

        # --- Initialize Metadata Files ---
        self._init_directory_with_metadata(
            os.path.join("data", "raw"), "Root raw_data directory"
        )
        self._init_directory_with_metadata(
            os.path.join("data", "raw", self.country_name),
            f"Root directory for {self.country_name}"
        )
        postal_code_dir = os.path.join("data", "raw", self.country_name, self.postal_code)
        self._init_directory_with_metadata(
            postal_code_dir,
            f"Data for postal code: {self.postal_code}"
        )
        care_type_dir = os.path.join(postal_code_dir, self.care_type)
        self._init_directory_with_metadata(
            care_type_dir,
            f"Data for care type: {self.care_type}"
        )
        self._init_directory_with_metadata(
            self.base_dir, # This is the sub_type directory
            f"Data for sub-type: {self.sub_type} within {self.care_type}"
        )
        self.logger.info(f"Initialized directory structure and metadata stubs up to: {self.base_dir}")

    # --- Logging and Metadata Methods (Adapted from Care.com example) ---

    def _setup_logging(self):
        """Configures logging to console (INFO) and file (DEBUG) in the base_dir."""
        log_file = os.path.join(self.base_dir, f"scrape_{self.sub_type}.log")
        
        # Prevent duplicate handlers if called multiple times (e.g., in tests)
        if hasattr(self, 'logger') and self.logger.hasHandlers():
            # If logger exists and has handlers, clear them before adding new ones
            # This might happen if __init__ is called again on the same instance
            self.logger.handlers.clear()

        self.logger = logging.getLogger(f"YoopiesScraper_{self.run_id}")
        self.logger.setLevel(logging.DEBUG) # Capture all levels

        # Avoid adding handlers if they already exist for this logger name
        if not self.logger.handlers:
            # File Handler (DEBUG level)
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)

            # Console Handler (INFO level)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)

            # Formatter
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        self.logger.info(f"Logger initialized. Log file: {log_file}")


    def _init_directory_with_metadata(self, dir_path: str, description: str) -> None:
        """Creates directory and initializes a basic metadata JSON file."""
        os.makedirs(dir_path, exist_ok=True)
        dir_name = os.path.basename(dir_path) if dir_path != os.path.join("data", "raw") else "raw_root"
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")

        # Don't overwrite if exists, just log
        if os.path.exists(metadata_file):
            # self.logger.debug(f"Metadata file already exists: {metadata_file}")
             return

        metadata_stub = {
            "deception": ( # Using the same 'deception' field name
                "This metadata file provides context for the data within this directory. "
                "It includes run ID, timestamps, status, and descriptive information "
                "for data lineage and understanding by future LLMs or engineers."
            ),
            "directory_name": dir_name,
            "directory_path": dir_path,
            "creation_time_utc": datetime.utcnow().isoformat() + "Z",
            "scrape_status": "initialized",
            "run_id": self.run_id, # Link to the specific run
            "description": description,
            "scraper_type": "YoopiesScraper",
        }

        try:
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_stub, f, ensure_ascii=False, indent=2)
            # self.logger.debug(f"Initialized metadata file: {metadata_file}")
        except Exception as e:
            self.logger.error(f"Failed to write initial metadata to {metadata_file}: {e}")

    def _update_directory_metadata(
        self,
        dir_path: str,
        scrape_status: str,
        error_message: Optional[str] = None,
        error_stage: Optional[str] = None,
        extra_data: Optional[dict] = None
    ) -> None:
        """Updates the metadata file in dir_path with status, errors, and extra info."""
        dir_name = os.path.basename(dir_path) if dir_path != os.path.join("data", "raw") else "raw_root"
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")
        
        metadata = {}
        # Load existing data if file exists
        if os.path.isfile(metadata_file):
            try:
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except json.JSONDecodeError:
                self.logger.error(f"Metadata file {metadata_file} is corrupted. Creating a new one.")
                metadata = {} # Start fresh if corrupted
            except Exception as e:
                 self.logger.error(f"Failed to read metadata file {metadata_file}: {e}. Creating a new one.")
                 metadata = {}


        # Ensure base fields are present if loaded data was empty or file didn't exist
        if not metadata:
             metadata = {
                "deception": "Context for data in this directory.",
                "directory_name": dir_name,
                "directory_path": dir_path,
                "creation_time_utc": datetime.utcnow().isoformat() + "Z", # Set creation time now
                "run_id": self.run_id,
                "scraper_type": "YoopiesScraper",
             }

        # Update core fields
        metadata["scrape_status"] = scrape_status
        metadata["last_update_time_utc"] = datetime.utcnow().isoformat() + "Z"
        metadata["run_id"] = self.run_id # Ensure run_id is updated/present

        if error_message:
            metadata["error_message"] = error_message
        elif "error_message" in metadata:
             del metadata["error_message"] # Clear previous error if status is now success

        if error_stage:
            metadata["error_stage"] = error_stage
        elif "error_stage" in metadata:
            del metadata["error_stage"]

        # Merge extra data
        if extra_data:
            metadata.update(extra_data)

        # Write back
        try:
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to update metadata file {metadata_file}: {e}")

    # --- Core Scraping Logic ---

    def _make_payload(self, search_after: int = 0) -> Dict[str, Any]:
        """Creates the GraphQL payload for a specific page request."""

        # Map care_type and sub_type to GraphQL variables/filters
        # This could be more sophisticated if needed
        service_map = {"childcare": "CHILDCARE", "tutoring": "TUTORING"} # Add others as needed
        type_filter_map = {"babysitting": "SITTER", "nanny": "NANNY"} # Add others as needed

        gql_service = service_map.get(self.care_type, "CHILDCARE") # Default if not mapped
        gql_type_filter = type_filter_map.get(self.sub_type, "SITTER") # Default if not mapped

        variables = {
            "after": "",
            "sortBy": "RELEVANCE", # Or make this a parameter
            "locale": self.locale,
            "filters": {
                "type": gql_type_filter,
                "sitterFilters": { # Assuming Sitter - adjust if scraping other types
                    "skills": [],
                    "distance": self.search_radius_km,
                    "parentAdType": [],
                    "specialAvailability": [],
                    "experienceYears": None,
                    "ageRange": {"ageMin": 16, "ageMax": 90}, # Example age range
                    "address": {
                        "country": "France", # Assuming France based on yoopies.fr
                        "countryCode": self.country_code,
                        "street": "",
                        "zipCode": self.postal_code,
                        "city": self.city,
                        "latitude": self.latitude,
                        "longitude": self.longitude,
                        "name": f"{self.city}, France", # Example name
                    },
                    "increaseDistance": True,
                }
                # Add other filter structures here if needed (e.g., 'teacherFilters')
            },
            "currentUserId": None, # Typically null for anonymous browsing
            "service": gql_service,
            "page": search_after, # Seems less relevant when using 'after' cursor, but included in example
            "first": self.search_page_size # Number of results per page
        }

        return {
            "query": self.QUERY,
            "variables": variables
        }

    def _fetch_all_profiles(self) -> None:
        """
        Fetches all profiles for the configured location and service type,
        handling pagination and saving data/metadata.
        """
        start_time = time.time()
        has_next_page = True
        search_after = 0 # Start with empty cursor for the first page "page"
        page_count = -1
        total_profiles_expected = 0 # Get from first response
        total_profiles_found = 0
        error_occurred = False
        first_request_payload = None # To store for metadata

        self.logger.info(
            f"Starting profile fetch for {self.postal_code} ({self.city}) - "
            f"{self.care_type}/{self.sub_type}. Run ID: {self.run_id}"
        )

        # Update metadata to 'running'
        self._update_directory_metadata(self.base_dir, scrape_status="running")

        while has_next_page:
            page_count += 1
            self.logger.info(f"Requesting page {page_count}...")
            payload = self._make_payload(search_after)

            if page_count == 0:
                first_request_payload = payload # Save for metadata

            try:
                response = self.session.post(self.GRAPHQL_URL, json=payload, timeout=90) # Increased timeout
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                data = response.json()

                # --- Basic Error Checking ---
                if "errors" in data and data["errors"]:
                    error_msg = f"GraphQL errors on page {page_count}: {data['errors']}"
                    self.logger.error(error_msg)
                    self._update_directory_metadata(
                        self.base_dir,
                        scrape_status="error",
                        error_message=error_msg,
                        error_stage=f"graphql_error_page_{page_count}"
                    )
                    error_occurred = True
                    break # Stop processing on GraphQL error

                ads_data = data.get("data", {}).get("ads")
                if not ads_data:
                    error_msg = f"Missing 'data.ads' structure in response on page {page_count}."
                    self.logger.error(error_msg)
                    self._update_directory_metadata(
                        self.base_dir,
                        scrape_status="error",
                        error_message=error_msg,
                        error_stage=f"missing_data_ads_page_{page_count}"
                    )
                    error_occurred = True
                    break

                # --- Process Data ---
                page_info = ads_data.get("pageInfo", {})
                edges = ads_data.get("edges", [])
                current_page_profiles = len(edges)
                total_profiles_found += current_page_profiles

                if page_count == 0:
                    total_profiles_expected = ads_data.get("count", 0)
                    self.logger.info(f"Total profiles expected for this search: {total_profiles_expected}")

                # --- Save Page Data ---
                page_file_path = os.path.join(self.base_dir, f"page_{page_count}.json")
                try:
                    with open(page_file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    self.logger.info(f"Saved page {page_count} ({current_page_profiles} profiles) to {page_file_path}")
                except Exception as e:
                    error_msg = f"Failed to save page {page_count} data to {page_file_path}: {e}"
                    self.logger.error(error_msg)
                    # Decide whether to stop or continue if saving fails
                    # For robustness, let's log the error and try to continue
                    # error_occurred = True # Optional: mark as error if saving is critical
                    # break               # Optional: stop if saving fails

                # --- Pagination Control ---
                has_next_page = page_info.get("hasNextPage", False)
                search_after = ads_data.get("currentPage", 0) + 1

                if has_next_page and not search_after:
                    self.logger.warning(f"hasNextPage is true on page {page_count}, but endCursor is missing. Stopping pagination.")
                    has_next_page = False # Prevent infinite loop

                if has_next_page:
                    self.logger.debug(f"Next page number: {search_after}")
                    # --- Rate Limiting / Politeness ---
                    sleep_time = round(random.uniform(0.5, 1.1), 2) # Slightly longer sleep
                    self.logger.debug(f"Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                     self.logger.info("No more pages indicated by server.")


            except requests.exceptions.Timeout:
                 error_msg = f"Request timed out on page {page_count}."
                 self.logger.error(error_msg)
                 self._update_directory_metadata(self.base_dir, scrape_status="error", error_message=error_msg, error_stage=f"timeout_page_{page_count}")
                 error_occurred = True
                 break # Stop on timeout
            except requests.exceptions.RequestException as e:
                 error_msg = f"HTTP request failed on page {page_count}: {e}"
                 self.logger.error(error_msg)
                 self._update_directory_metadata(self.base_dir, scrape_status="error", error_message=str(e), error_stage=f"http_error_page_{page_count}")
                 error_occurred = True
                 break # Stop on other request errors
            except json.JSONDecodeError as e:
                 error_msg = f"Failed to decode JSON response on page {page_count}: {e}"
                 self.logger.error(error_msg)
                 # Optionally save the raw response text here for debugging
                 # with open(os.path.join(self.base_dir, f"page_{page_count}_error.txt"), "w") as f:
                 #    f.write(response.text)
                 self._update_directory_metadata(self.base_dir, scrape_status="error", error_message=error_msg, error_stage=f"json_decode_error_page_{page_count}")
                 error_occurred = True
                 break # Stop on bad JSON
            except Exception as e: # Catch any other unexpected errors
                 error_msg = f"An unexpected error occurred on page {page_count}: {e}"
                 self.logger.exception(error_msg) # Use exception to log traceback
                 self._update_directory_metadata(self.base_dir, scrape_status="error", error_message=str(e), error_stage=f"unexpected_error_page_{page_count}")
                 error_occurred = True
                 break

        # --- Final Metadata Update ---
        end_time = time.time()
        duration_seconds = round(end_time - start_time, 2)

        final_status = "error" if error_occurred else "success"
        self.logger.info(
            f"Finished fetching profiles. Final Status: {final_status}. "
            f"Total pages processed: {page_count+1}. Total profiles found: {total_profiles_found}. "
            f"Duration: {duration_seconds}s."
        )

        # Prepare metadata payload
        # Sanitize headers (remove cookie)
        safe_headers = {k: v for k, v in self.session.headers.items() if k.lower() != "cookie"}

        # Sanitize payload (remove sensitive parts if any - query vars look okay here)
        safe_payload = first_request_payload # Use the first request as sample

        scraper_info = {
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "requests_version": requests.__version__,
        }

        final_metadata = {
            "start_time_utc": datetime.utcfromtimestamp(start_time).isoformat() + "Z",
            "end_time_utc": datetime.utcfromtimestamp(end_time).isoformat() + "Z",
            "duration_seconds": duration_seconds,
            "total_pages_processed": page_count,
            "total_profiles_found": total_profiles_found,
            "total_profiles_expected": total_profiles_expected if page_count > 0 else 0,
            "search_parameters": { # Record key search params
                 "postal_code": self.postal_code,
                 "city": self.city,
                 "latitude": self.latitude,
                 "longitude": self.longitude,
                 "country_code": self.country_code,
                 "care_type": self.care_type,
                 "sub_type": self.sub_type,
                 "search_radius_km": self.search_radius_km,
                 "locale": self.locale,
                 "page_size": self.search_page_size,
            },
            "api_request_sample": { # Store a sample request (first page)
                "url": self.GRAPHQL_URL,
                "headers": safe_headers,
                "payload": safe_payload
            } if safe_payload else None,
            "scraper_info": scraper_info,
        }

        # Update the primary metadata file for this specific scrape task (in the sub_type dir)
        self._update_directory_metadata(
             dir_path=self.base_dir,
             scrape_status=final_status,
             # Error details are already set if error occurred
             extra_data=final_metadata
        )

    def run(self):
        """Main entry point to start the scraping process."""
        self.logger.info(f"--- Starting Yoopies Scrape Run ---")
        self.logger.info(f"Run ID: {self.run_id}")
        self.logger.info(f"Target: {self.country_name}/{self.postal_code}/{self.care_type}/{self.sub_type}")

        try:
            self._fetch_all_profiles()
            # If _fetch_all_profiles finished without throwing an exception,
            # the status in the metadata file reflects the outcome (success/error).
            self.logger.info(f"--- Yoopies Scrape Run Finished ---")

        except Exception as e:
            # Catch unexpected errors during the setup or if _fetch_all_profiles re-raises
            self.logger.exception(f"Critical error during scraper run: {e}")
            # Ensure the base directory metadata reflects this top-level failure
            self._update_directory_metadata(
                dir_path=self.base_dir,
                scrape_status="error",
                error_message=f"Critical failure in run(): {e}",
                error_stage="main_run_method"
            )
            self.logger.info(f"--- Yoopies Scrape Run Failed Critically ---")
            
        
    def scrape():
        print("Running YoopiesScraper example...")
        # --- IMPORTANT: Replace with actual coordinates for the postal code ---
        # You might use a geocoding service/library (like geopy) or a predefined mapping
        # Example for Paris 75000:
        TARGET_POSTAL_CODE = "75000"
        TARGET_CITY = "Paris"
        TARGET_LATITUDE = 48.8534 # Approximate center for Paris
        TARGET_LONGITUDE = 2.3488 # Approximate center for Paris
    
        # Example for Bordeaux 33000:
        # TARGET_POSTAL_CODE = "33000"
        # TARGET_CITY = "Bordeaux"
        # TARGET_LATITUDE = 44.8412
        # TARGET_LONGITUDE = -0.5800
    
        # Ensure YOOPIES_COOKIE is correctly set in config.py or the variable above
        if not YOOPIES_COOKIE or "YOUR_YOOPIES_COOKIE_STRING" in YOOPIES_COOKIE:
             print("\nERROR: Please set your Yoopies cookie in config.py or directly in the script.")
             print("Example: YOOPIES_COOKIE = '_ga=GA1...; PHPSESSID=...;'")
             sys.exit(1) # Exit if cookie isn't set
    
        try:
            scraper = YoopiesScraper(
                postal_code=TARGET_POSTAL_CODE,
                city=TARGET_CITY,
                latitude=TARGET_LATITUDE,
                longitude=TARGET_LONGITUDE,
                care_type="childcare",    # Corresponds to 'garde d'enfants'
                sub_type="babysitting",   # Corresponds to 'baby-sitting'
                search_page_size=100,     # Request 100 per page
                cookie=YOOPIES_COOKIE     # Pass the cookie
            )
            scraper.run()
            print("\nScraping process initiated. Check logs and data directory.")
    
        except ValueError as ve: # Catch specific init errors like missing cookie
             print(f"\nConfiguration Error: {ve}")
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}")
            # Log exception if logger was initialized, otherwise just print
            try:
                # Use root logger if scraper.logger isn't available
                logging.getLogger().exception("Scraper execution failed in __main__")
            except: # Catch all logging exceptions
                 pass



# ------------------------------------------------------------------------------
# EXAMPLE USAGE
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    print("Running YoopiesScraper example...")

    # --- IMPORTANT: Replace with actual coordinates for the postal code ---
    # You might use a geocoding service/library (like geopy) or a predefined mapping
    # Example for Paris 75000:
    TARGET_POSTAL_CODE = "75000"
    TARGET_CITY = "Paris"
    TARGET_LATITUDE = 48.8534 # Approximate center for Paris
    TARGET_LONGITUDE = 2.3488 # Approximate center for Paris

    # Example for Bordeaux 33000:
    # TARGET_POSTAL_CODE = "33000"
    # TARGET_CITY = "Bordeaux"
    # TARGET_LATITUDE = 44.8412
    # TARGET_LONGITUDE = -0.5800

    # Ensure YOOPIES_COOKIE is correctly set in config.py or the variable above
    if not YOOPIES_COOKIE or "YOUR_YOOPIES_COOKIE_STRING" in YOOPIES_COOKIE:
         print("\nERROR: Please set your Yoopies cookie in config.py or directly in the script.")
         print("Example: YOOPIES_COOKIE = '_ga=GA1...; PHPSESSID=...;'")
         sys.exit(1) # Exit if cookie isn't set

    try:
        scraper = YoopiesScraper(
            postal_code=TARGET_POSTAL_CODE,
            city=TARGET_CITY,
            latitude=TARGET_LATITUDE,
            longitude=TARGET_LONGITUDE,
            care_type="childcare",    # Corresponds to 'garde d'enfants'
            sub_type="babysitting",   # Corresponds to 'baby-sitting'
            search_page_size=100,     # Request 100 per page
            cookie=YOOPIES_COOKIE     # Pass the cookie
        )
        scraper.run()
        print("\nScraping process initiated. Check logs and data directory.")

    except ValueError as ve: # Catch specific init errors like missing cookie
         print(f"\nConfiguration Error: {ve}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        # Log exception if logger was initialized, otherwise just print
        try:
            # Use root logger if scraper.logger isn't available
            logging.getLogger().exception("Scraper execution failed in __main__")
        except: # Catch all logging exceptions
             pass
         
# ------------------------------------------------------------------------------
# Scrape function wrapper
# ------------------------------------------------------------------------------

def scrape():
    print("Running YoopiesScraper example...")
    # --- IMPORTANT: Replace with actual coordinates for the postal code ---
    # You might use a geocoding service/library (like geopy) or a predefined mapping
    # Example for Paris 75000:
    TARGET_POSTAL_CODE = "75000"
    TARGET_CITY = "Paris"
    TARGET_LATITUDE = 48.8534 # Approximate center for Paris
    TARGET_LONGITUDE = 2.3488 # Approximate center for Paris

    # Example for Bordeaux 33000:
    # TARGET_POSTAL_CODE = "33000"
    # TARGET_CITY = "Bordeaux"
    # TARGET_LATITUDE = 44.8412
    # TARGET_LONGITUDE = -0.5800

    # Ensure YOOPIES_COOKIE is correctly set in config.py or the variable above
    if not YOOPIES_COOKIE or "YOUR_YOOPIES_COOKIE_STRING" in YOOPIES_COOKIE:
         print("\nERROR: Please set your Yoopies cookie in config.py or directly in the script.")
         print("Example: YOOPIES_COOKIE = '_ga=GA1...; PHPSESSID=...;'")
         sys.exit(1) # Exit if cookie isn't set

    try:
        scraper = YoopiesScraper(
            postal_code=TARGET_POSTAL_CODE,
            city=TARGET_CITY,
            latitude=TARGET_LATITUDE,
            longitude=TARGET_LONGITUDE,
            care_type="childcare",    # Corresponds to 'garde d'enfants'
            sub_type="babysitting",   # Corresponds to 'baby-sitting'
            search_page_size=100,     # Request 100 per page
            search_radius_km= 9000,   # Set a larger search radius
            cookie=YOOPIES_COOKIE     # Pass the cookie
        )
        scraper.run()
        print("\nScraping process initiated. Check logs and data directory.")

    except ValueError as ve: # Catch specific init errors like missing cookie
         print(f"\nConfiguration Error: {ve}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        # Log exception if logger was initialized, otherwise just print
        try:
            # Use root logger if scraper.logger isn't available
            logging.getLogger().exception("Scraper execution failed in __main__")
        except: # Catch all logging exceptions
             pass
