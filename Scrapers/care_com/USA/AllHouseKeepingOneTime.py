"""
CareScraper: DataOps-Oriented Web Scraping Pipeline for Care.com with Logging

PURPOSE:
  1) Demonstrate how to scrape in home (Housekeeping) data from Care.com, respecting a 500-record
     limit per query by splitting the pay range if needed.
  2) Showcase a DataOps approach by creating metadata files (JSON) in each directory under raw_data/
     to record run details, error states, and an "api_request" field (excluding cookies).
  3) Additionally, capture all console output in a dedicated log file using Python’s logging module.

DIRECTORY STRUCTURE:
  raw_data/
    <country_name>/
      metadata_<country_name>.json
      <postal_code>/
        metadata_<postal_code>.json
        search_of_<care_type>/
          metadata_search_of_<care_type>.json
          <sub_type>/
            metadata_<sub_type>.json
            range_<pay_min>_<pay_max>/
              page_1.json
              page_2.json
              ...
              metadata_range_<pay_min>_<pay_max>.json

LOGGING:
  - A single log file (scrape_<sub_type>.log) is placed in the <sub_type> directory.
  - All print statements are replaced by logger calls, which also go to console.

METADATA & ERROR HANDLING:
  - Each directory has a minimal or detailed metadata file named metadata_<directory_name>.json.
  - On errors, we store scrape_status = "error", plus error_stage and error_message for debugging.
  - The "api_request" field in range metadata includes headers (minus cookies) and query variables.

'DECEPTION' FIELD:
  - Each metadata file has a "deception" field, explaining how future LLMs or data engineers should
    interpret the file’s structure and purpose.

DISCLAIMER:
  - This code is for educational purposes. Excessive scraping can lead to IP blocking or other actions.
  - Always respect the site's Terms of Service.

Author: @AyoubFrihaoui
Version: 3.0.0
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
from datetime import datetime
from typing import Tuple, List, Optional
from config import COOKIE

# If you store the cookie in config.py, import it:
# from config import COOKIE

#COOKIE = "YOUR_COOKIE_STRING"  # Replace if not using an external config

class AllHouseKeepingOneTime:
    """
    A pipeline to scrape babysitting (housekeeping) data from Care.com using a
    GraphQL endpoint. Incorporates DataOps practices (metadata JSON files)
    and logs all console output to a file.
    """

    # GraphQL endpoint
    GRAPHQL_URL = "https://www.care.com/api/graphql"

    # HTTP headers (excluding sensitive data from logs)
    HEADERS = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,ar;q=0.8",
        "apollographql-client-name": "search-mfe",
        "apollographql-client-version": "1.230.0",
        "baggage": "sentry-environment=prod,sentry-release=search-mfe%401.230.0,sentry-public_key=74fca7c56b254f19bda68008acb52ac3,sentry-trace_id=ae21d2ad507b4e1d81cd4678777741a1,sentry-sample_rate=0.4,sentry-transaction=%2F,sentry-sampled=false",
        "content-type": "application/json",
        "origin": "https://www.care.com",
        "priority": "u=1, i",
        "referer": "https://www.care.com/app/search?zipcode=07008&radius=10&vertical=SENIOR_CARE&subvertical=COMPANION&ageRanges=48+-+71%2C0+-+11%2C12+-+47%2C144+-+216%2C72+-+143&numberOfChildren=1&attributes=CPR_TRAINED&languages=ENGLISH&maxPayRate=44&minPayRate=15",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sentry-trace": "ae21d2ad507b4e1d81cd4678777741a1-afd3d58512ed97dd-0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Cookie": COOKIE
    }

    # GraphQL query (excluding auth tokens)
    QUERY = """
fragment CaregiverFragment on SearchProvidersSuccess {
  sourceType
  searchProvidersConnection {
    pageInfo {
      hasNextPage
      endCursor
      __typename
    }
    totalHits
    edges {
      node {
        ... on Caregiver {
          member {
            id
            legacyId
            imageURL
            displayName
            firstName
            lastName
            address {
              city
              state
              zip
              __typename
            }
            primaryService
            __typename
          }
          hasCareCheck
          badges
          yearsOfExperience
          profileDataSource
          hiredByCounts {
            locality {
              hiredCount
              __typename
            }
            __typename
          }
          hiredTimes
          revieweeMetrics {
            ... on ReviewFailureResponse {
              message
              __typename
            }
            ... on RevieweeMetricsPayload {
              metrics {
                totalReviews
                averageRatings {
                  type
                  value
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          profiles {
            commonCaregiverProfile {
              id
              bio {
                experienceSummary
                __typename
              }
              __typename
            }
            childCareCaregiverProfile {
              rates {
                hourlyRate {
                  amount
                  __typename
                }
                numberOfChildren
                __typename
              }
              recurringRate {
                hourlyRateFrom {
                  amount
                  __typename
                }
                hourlyRateTo {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            petCareCaregiverProfile {
              serviceRates {
                duration
                rate {
                  amount
                  __typename
                }
                subtype
                __typename
              }
              recurringRate {
                hourlyRateFrom {
                  amount
                  __typename
                }
                hourlyRateTo {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            houseKeepingCaregiverProfile {
              recurringRate {
                hourlyRateFrom {
                  amount
                  __typename
                }
                hourlyRateTo {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            tutoringCaregiverProfile {
              recurringRate {
                hourlyRateFrom {
                  amount
                  __typename
                }
                hourlyRateTo {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            seniorCareCaregiverProfile {
              recurringRate {
                hourlyRateFrom {
                  amount
                  __typename
                }
                hourlyRateTo {
                  amount
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          featuredReview {
            description {
              displayText
              originalText
              __typename
            }
            reviewer {
              publicMemberInfo {
                firstName
                lastInitial
                imageURL
                __typename
              }
              __typename
            }
            __typename
          }
          isFavorite
          __typename
        }
        ... on SearchProvidersNodeError {
          providerId
          message
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

query SearchProvidersHousekeeping($input: SearchProvidersHousekeepingInput!) {
  searchProvidersHousekeeping(input: $input) {
    ... on SearchProvidersSuccess {
      ...CaregiverFragment
      __typename
    }
    ... on SearchProvidersError {
      message
      __typename
    }
    __typename
  }
}
"""

    def __init__(
        self,
        country_name: str = "USA",
        postal_code: str = "07008",
        care_type: str = "housekeeping",
        sub_type: str = "onetime",
        search_page_size: int = 10,
        min_pay_range: int = 0,
        max_pay_range: int = 100
    ):
        """
        :param country_name: e.g., "USA"
        :param postal_code: e.g., "07008"
        :param care_type: e.g., "housekeeping"
        :param sub_type: e.g., "onetime"
        :param search_page_size: number of caregivers per page
        :param min_pay_range: global min boundary for pay range
        :param max_pay_range: global max boundary for pay range
        """
        self.country_name = country_name
        self.postal_code = postal_code
        self.care_type = care_type
        self.sub_type = sub_type
        self.search_page_size = search_page_size
        self.min_pay_range = min_pay_range
        self.max_pay_range = max_pay_range

        # Unique run ID for the entire scrape
        self.run_id = str(uuid.uuid4())

        # Directory structure:
        #   raw_data/<country_name>/<postal_code>/search_of_<care_type>/<sub_type>/
        self.base_dir = os.path.join(
            "raw_data",
            self.country_name,
            self.postal_code,
            f"search_of_{self.care_type.lower()}",
            self.sub_type
        )
        os.makedirs(self.base_dir, exist_ok=True)

        # Initialize logging to both console and file
        self._setup_logging()

        # Initialize stub metadata for each directory in the path
        self._init_directory_with_metadata(
            os.path.join("raw_data"), "Root raw_data directory"
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", self.country_name),
            f"Country: {self.country_name}"
        )
        self._init_directory_with_metadata(
            os.path.join("raw_data", self.country_name, self.postal_code),
            f"Postal code: {self.postal_code}"
        )
        care_type_dir = os.path.join(
            "raw_data", self.country_name, self.postal_code, f"search_of_{self.care_type.lower()}"
        )
        self._init_directory_with_metadata(care_type_dir, f"Care type: {self.care_type}")

        self._init_directory_with_metadata(
            self.base_dir,
            f"Sub-type: {self.sub_type}, under {self.care_type}"
        )

    def _setup_logging(self):
        """
        Configure a logger to output to both console and a file named scrape_<sub_type>.log
        inside the sub_type directory.
        """
        log_file = os.path.join(self.base_dir, f"scrape_{self.sub_type}.log")

        # Create a custom logger
        self.logger = logging.getLogger(self.run_id)
        self.logger.setLevel(logging.DEBUG)

        # File handler (DEBUG level)
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)

        # Console handler (INFO level)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Attach handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.info(f"Log file initialized at: {log_file}")

    def _init_directory_with_metadata(self, dir_path: str, description: str) -> None:
        """
        Create a directory (if it doesn't exist) and store a minimal metadata file named
        metadata_<directory_name>.json. This ensures each directory under raw_data has
        a DataOps context.
        """
        os.makedirs(dir_path, exist_ok=True)
        dir_name = os.path.basename(dir_path)
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")

        # Minimal metadata stub
        metadata_stub = {
            "deception": (
                "This metadata file is used by future LLMs or data engineers to understand "
                "the structure and purpose of the data in this directory."
            ),
            "directory_name": dir_name,
            "creation_time": datetime.utcnow().isoformat() + "Z",
            "scrape_status": "initialized",
            "run_id": self.run_id,
            "description": description
        }

        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata_stub, f, ensure_ascii=False, indent=2)

    def _update_directory_metadata(
        self,
        dir_path: str,
        scrape_status: str,
        error_message: Optional[str] = None,
        error_stage: Optional[str] = None,
        extra_data: Optional[dict] = None
    ) -> None:
        """
        Update (or create) the metadata file in dir_path, adjusting fields like
        'scrape_status', adding error details, or extra_data.
        """
        dir_name = os.path.basename(dir_path)
        metadata_file = os.path.join(dir_path, f"metadata_{dir_name}.json")

        # Load existing metadata if present
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
                "scrape_status": "initialized",
                "run_id": self.run_id
            }

        # Update status and any error info
        metadata["scrape_status"] = scrape_status
        if error_message:
            metadata["error_message"] = error_message
        if error_stage:
            metadata["error_stage"] = error_stage

        # Merge extra fields
        if extra_data:
            for k, v in extra_data.items():
                metadata[k] = v

        # Write back to disk
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

    def _make_payload(self, search_after: str, pay_min: int, pay_max: int) -> dict:
        """
        Create the GraphQL payload for the given pay range and searchAfter cursor.
        We'll exclude cookies from metadata, but store everything else if needed.
        """
        return {
            "query": self.QUERY,
            "variables": {
                "input": {
                    "careType": "ONE_TIME",  # specifically nanny
                    "filters": {
                        "payRange": {
                            "min": {
                                "amount": pay_min,
                                "currencyCode": "USD"
                            },
                            "max": {
                                "amount": pay_max,
                                "currencyCode": "USD"
                            }
                        },
                        "postalCode": self.postal_code,
                        "searchPageSize": self.search_page_size,
                        "searchAfter": search_after
                    }
                }
            }
        }

    def _get_total_hits_for_range(self, pay_min: int, pay_max: int) -> int:
        """
        Query with pageSize=1 to quickly retrieve totalHits for the range.
        Return 999999 if an error occurs, forcing a further split.
        """
        payload = self._make_payload("", pay_min, pay_max)
        payload["variables"]["input"]["filters"]["searchPageSize"] = 1

        try:
            response = requests.post(self.GRAPHQL_URL, headers=self.HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()

            time.sleep(round(random.uniform(1.5, 1.8), 4))

            if "errors" in data:
                self.logger.error(f"GraphQL errors in _get_total_hits_for_range: {data['errors']}")
                return 999999

            senior_care_data = data["data"]["searchProvidersHousekeeping"]
            connection = senior_care_data.get("searchProvidersConnection", {})
            total_hits = connection.get("totalHits", 0)
            return total_hits

        except Exception as e:
            self.logger.error(f"Exception in _get_total_hits_for_range: {e}")
            return 999999

    def _fetch_profiles_for_range(self, pay_min: int, pay_max: int):
        """
        Paginate through [pay_min, pay_max], storing JSON pages in
        raw_data/<country_name>/<postal_code>/search_of_<care_type>/<sub_type>/range_<pay_min>_<pay_max>.
        Also writes a metadata file named metadata_range_<pay_min>_<pay_max>.json.

        This method logs all steps to the logger and handles error states in metadata.
        """
        range_dir_name = f"range_{pay_min}_{pay_max}"
        range_dir = os.path.join(self.base_dir, range_dir_name)
        os.makedirs(range_dir, exist_ok=True)

        # Initialize the range directory metadata
        self._init_directory_with_metadata(
            range_dir, f"Range directory for {pay_min}-{pay_max} USD"
        )

        start_time = time.time()
        session = requests.Session()
        session.headers.update(self.HEADERS)

        has_next_page = True
        search_after = ""
        page_count = 0
        total_caregivers = 0

        scrape_status = "success"
        error_message = None
        error_stage = None

        self.logger.info(f"Fetching data for pay range [{pay_min}, {pay_max}]...")

        try:
            while has_next_page:
                page_count += 1
                payload = self._make_payload(search_after, pay_min, pay_max)
                response = session.post(self.GRAPHQL_URL, json=payload)
                response.raise_for_status()

                data = response.json()

                # Random sleep to avoid detection
                time.sleep(round(random.uniform(1.5, 1.8), 4))

                if "errors" in data:
                    scrape_status = "error"
                    error_message = f"GraphQL errors on page {page_count}: {data['errors']}"
                    error_stage = f"range_{pay_min}_{pay_max}_page_{page_count}"
                    self.logger.error(error_message)
                    break

                senior_care_data = data["data"]["searchProvidersHousekeeping"]
                connection = senior_care_data.get("searchProvidersConnection", {})
                edges = connection.get("edges", [])
                page_info = connection.get("pageInfo", {})

                num_edges = len(edges)
                total_caregivers += num_edges

                # Save page data
                page_file = os.path.join(range_dir, f"page_{page_count}.json")
                with open(page_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                self.logger.info(
                    f"  - Page {page_count} => {num_edges} caregivers, saved to {page_file}"
                )

                has_next_page = page_info.get("hasNextPage", False)
                search_after = page_info.get("endCursor", "")

                if has_next_page:
                    self.logger.info(f"  - Next endCursor: {search_after}")

        except Exception as e:
            scrape_status = "error"
            error_message = str(e)
            error_stage = f"range_{pay_min}_{pay_max}_pagination"
            self.logger.error(f"Exception while scraping range [{pay_min}, {pay_max}]: {e}")

        end_time = time.time()
        duration_seconds = round(end_time - start_time, 2)

        self.logger.info(
            f"Finished range [{pay_min}, {pay_max}] with status='{scrape_status}'. "
            f"Total pages: {page_count}, total caregivers: {total_caregivers}."
        )

        # Build a "headers minus cookies" for the metadata
        safe_headers = {k: v for k, v in self.HEADERS.items() if k.lower() != "cookie"}
        # Example variables for the final request (excluding search_after which changes each page)
        sample_payload = self._make_payload("", pay_min, pay_max)

        # Collect environment or other extra info
        scraper_info = {
            "python_version": sys.version,
            "platform": platform.platform(),
            "some_future_data": "Any additional data we might store for data lineage"
        }

        # Save final metadata
        extra_meta = {
            "start_time_iso": datetime.utcfromtimestamp(start_time).isoformat() + "Z",
            "end_time_iso": datetime.utcfromtimestamp(end_time).isoformat() + "Z",
            "duration_seconds": duration_seconds,
            "total_pages": page_count,
            "total_caregivers": total_caregivers,
            # Rename 'graphql_query' to 'api_request'
            "api_request": {
                "headers": safe_headers,
                "payload": sample_payload
            },
            "scraper_info": scraper_info
        }

        self._update_directory_metadata(
            dir_path=range_dir,
            scrape_status=scrape_status,
            error_message=error_message,
            error_stage=error_stage,
            extra_data=extra_meta
        )

    def run(self):
        """
        Main entry point:
          1) Mark top-level directory as "initialized".
          2) For the global pay range [min_pay_range, max_pay_range], check totalHits.
          3) If totalHits > 500, split the range and re-check each subrange.
          4) If totalHits <= 500, paginate that subrange with _fetch_profiles_for_range.
          5) Log all progress to a file and console.
        """
        self.logger.info(
            f"Starting scraping for {self.country_name}/{self.postal_code}, "
            f"care_type='{self.care_type}', sub_type='{self.sub_type}', "
            f"pay range [{self.min_pay_range}, {self.max_pay_range}]."
        )
        self.logger.info(f"Run ID: {self.run_id}")

        # Mark the sub_type directory as "initialized"
        self._update_directory_metadata(
            self.base_dir,
            scrape_status="initialized"
        )

        try:
            ranges_to_check: List[Tuple[int, int]] = [(self.min_pay_range, self.max_pay_range)]

            while ranges_to_check:
                pay_min, pay_max = ranges_to_check.pop(0)
                self.logger.info(f"Checking pay range [{pay_min}, {pay_max}]...")

                total_hits = self._get_total_hits_for_range(pay_min, pay_max)
                self.logger.info(f"  => totalHits = {total_hits}")

                if total_hits > 500:
                    if pay_min == pay_max:
                        self.logger.warning(
                            f"Single-value range [{pay_min}] but hits > 500. Skipping."
                        )
                        continue

                    mid = (pay_min + pay_max) // 2
                    left_range = (pay_min, mid)
                    right_range = (mid + 1, pay_max)

                    self.logger.info(
                        f"  => Splitting into [{left_range[0]}, {left_range[1]}] "
                        f"and [{right_range[0]}, {right_range[1]}]."
                    )
                    ranges_to_check.append(left_range)
                    ranges_to_check.append(right_range)
                elif total_hits == 0:
                    self.logger.info(
                        f"No results found for pay range [{pay_min}, {pay_max}]."
                    )
                else:
                    # totalHits <= 500, scrape this segment
                    self._fetch_profiles_for_range(pay_min, pay_max)

            # If we get here with no top-level errors
            self._update_directory_metadata(
                self.base_dir,
                scrape_status="success"
            )
            self.logger.info("All pay range segments processed. Scraping complete!")

        except Exception as e:
            self.logger.error(f"Unexpected error in run(): {e}")
            # Mark the sub_type directory with an error
            self._update_directory_metadata(
                self.base_dir,
                scrape_status="error",
                error_message=str(e),
                error_stage="top_level_run"
            )
            raise  # Re-raise the exception so it's not silently swallowed


# ------------------------------------------------------------------------------
# EXAMPLE USAGE
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    scraper = AllHouseKeepingOneTime(
        country_name="USA",
        postal_code="07008",
        care_type="housekeeping",
        sub_type="onetime",
        search_page_size=10,
        min_pay_range=0,
        max_pay_range=100
    )
    scraper.run()
