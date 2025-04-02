"""
ReviewScraper with Pagination:
Fetch caregiver reviews for multiple care types, handling multiple pages
via nextPageToken. Stores each page in a separate file:

  raw_data/USA/<postal_code>/reviews/<careType>/<caretakerID>_<careType>_<pageNumber>.json
  
PROCESS:
  1) Scan 'raw_data/USA/<postal_code>/all_profiles/' to collect caretaker IDs.
  2) For each caretaker ID, do 3 requests:
       - careType = "CHILD_CARE"
       - careType = "SENIOR_CARE"
       - careType = "HOUSEKEEPING"
  3) Save JSON to 'raw_data/USA/<postal_code>/reviews/<careType>/<caretakerID>_<careType>_<pageNumber>.json'
  4) Create a single log file in 'raw_data/USA/<postal_code>/reviews/scrape_reviews.log'.
  5) A metadata file is also created for each page, named: 'metadata_<caretakerID>_<careType>_<pageNumber>.json'

DISCLAIMER:
  - For demonstration. Always respect Care.com’s Terms of Service.
  - Adjust code for your environment, error handling, or additional data (pagination, etc.).
"""

import os
import sys
import json
import time
import random
import logging
import uuid
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List

# ------------------------------------------------------------------------------
# 1) HEADERS & GRAPHQL QUERY
# ------------------------------------------------------------------------------
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "apollographql-client-name": "search-mfe",
    "apollographql-client-version": "1.230.0",
    "baggage": (
        "sentry-environment=prod,"
        "sentry-release=search-mfe@1.230.0,"
        "sentry-public_key=74fca7c56b254f19bda68008acb52ac3,"
        "sentry-trace_id=e1b40cdf6bd5404899e8aa22938c69c8,"
        "sentry-sample_rate=0.4,"
        "sentry-transaction=/,"
        "sentry-sampled=false"
    ),
    "content-type": "application/json",
    "origin": "https://www.care.com",
    "priority": "u=1, i",
    "referer": "https://www.care.com/app/search",
    "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sentry-trace": "e1b40cdf6bd5404899e8aa22938c69c8-8741747362ac06bd-0",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Cookie": "YOUR_SESSION_COOKIE"  # Replace or import from config if needed
}

GRAPHQL_URL = "https://www.care.com/api/graphql"

REVIEWS_QUERY = """
query ReviewsByReviewee($revieweeId: ID!, $revieweeType: ReviewInfoEntityType!, $careType: ReviewInfoCareType, $pageSize: Int, $pageToken: String) {
  reviewsByReviewee(
    revieweeId: $revieweeId
    revieweeType: $revieweeType
    careType: $careType
    pageSize: $pageSize
    pageToken: $pageToken
  ) {
    ... on ReviewsByRevieweePayload {
      __typename
      nextPageToken
      reviews {
        attributes {
          truthy
          type
          __typename
        }
        careType
        createTime
        deleteTime
        description {
          displayText
          originalText
          __typename
        }
        id
        languageCode
        originalSource
        ratings {
          type
          value
          __typename
        }
        retort {
          displayText
          originalText
          __typename
        }
        reviewee {
          id
          providerType
          type
          __typename
        }
        reviewer {
          imageURL
          publicMemberInfo {
            firstName
            lastInitial
            __typename
          }
          source
          type
          __typename
        }
        status
        updateSource
        updateTime
        verifiedByCare
        __typename
      }
    }
    ... on ReviewFailureResponse {
      message
      __typename
    }
    __typename
  }
}
"""

# ------------------------------------------------------------------------------
# 2) UTILITY FUNCTIONS
# ------------------------------------------------------------------------------
def setup_logger(log_dir: str) -> logging.Logger:
    """
    Create or retrieve a logger that writes to both console and
    a file named 'scrape_reviews.log' in 'log_dir'.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "scrape_reviews.log")

    logger = logging.getLogger(f"reviews_{log_dir}")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicating handlers if logger is already set up
    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        fh.setLevel(logging.DEBUG)

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

def build_review_path(postal_code: str, care_type: str, caregiver_id: str, page_number: int) -> str:
    """
    e.g. raw_data/USA/<postal_code>/reviews/<careType>/<caregiverID>_<careType>_<pageNumber>.json
    """
    base_dir = os.path.join("raw_data", "USA", postal_code, "reviews", care_type)
    os.makedirs(base_dir, exist_ok=True)
    filename = f"{caregiver_id}_{care_type}_{page_number}.json"
    return os.path.join(base_dir, filename)

def save_json(file_path: str, data: Dict[str, Any]) -> None:
    """
    Save data as JSON to 'file_path'. Overwrites if file exists.
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def save_metadata(directory: str, caregiver_id: str, care_type: str, page_number: int, run_id: str, status: str, error: Optional[str] = None) -> None:
    """
    Save a minimal metadata file named 'metadata_<caregiverID>_<careType>_<pageNumber>.json' in 'directory'.
    """
    meta_name = f"metadata_{caregiver_id}_{care_type}_{page_number}.json"
    meta_path = os.path.join(directory, meta_name)
    meta = {
        "run_id": run_id,
        "caregiver_id": caregiver_id,
        "care_type": care_type,
        "page_number": page_number,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "scrape_status": status,
        "error_message": error,
        "notes": "Review scraping with pagination"
    }
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Could not save metadata to {meta_path}: {e}")

# ------------------------------------------------------------------------------
# 3) REVIEW SCRAPER CLASS (WITH PAGINATION)
# ------------------------------------------------------------------------------
class ReviewScraper:
    """
    1) Finds caregiver IDs in 'raw_data/USA/<postal_code>/all_profiles/'
    2) For each caregiver, queries 3 care types: "CHILD_CARE", "SENIOR_CARE", "HOUSEKEEPING"
       and fetches multiple pages if nextPageToken is set.
    3) Stores each page as:
         raw_data/USA/<postal_code>/reviews/<careType>/<caregiverID>_<careType>_<pageNumber>.json
    4) Logs in 'raw_data/USA/<postal_code>/reviews/scrape_reviews.log'
    """

    def __init__(self, postal_code: str):
        """
        :param postal_code: The ZIP/postal code to target (under 'raw_data/USA/<postal_code>/all_profiles/').
        """
        self.postal_code = postal_code
        self.run_id = str(uuid.uuid4())
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        # Create a logger for all review scraping logs under the postal code
        reviews_dir = os.path.join("raw_data", "USA", self.postal_code, "reviews")
        self.logger = setup_logger(reviews_dir)

    def load_caregiver_ids(self) -> List[str]:
        """
        Scan 'raw_data/USA/<postal_code>/all_profiles/' for caregiver profile JSON files.
        Return a list of caregiver IDs.
        """
        base_dir = os.path.join("raw_data", "USA", self.postal_code, "all_profiles")
        caregiver_ids = []

        for dirpath, _, filenames in os.walk(base_dir):
            for filename in filenames:
                if filename.endswith(".json") and not filename.startswith("metadata_"):
                    cid = filename.replace(".json", "")
                    caregiver_ids.append(cid)
        
        self.logger.info(f"Found {len(caregiver_ids)} caregiver JSON files before deduplication.")
        caregiver_ids = list(set(caregiver_ids))
        self.logger.info(f"Found {len(caregiver_ids)} unique caregiver IDs after deduplication.")

        return caregiver_ids

    def fetch_reviews_page(
        self,
        caregiver_id: str,
        care_type: str,
        page_size: int = 10,
        page_token: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single page of reviews for caregiver_id with given care_type.
        :param caregiver_id: The caregiver's ID (revieweeId).
        :param care_type: e.g. "CHILD_CARE", "SENIOR_CARE", "HOUSEKEEPING".
        :param page_size: Number of reviews to request (default=10).
        :param page_token: If provided, fetch next page using this token.
        :return: The parsed JSON if successful, or None if there's an error or GraphQL errors.
        """
        variables = {
            "revieweeId": caregiver_id,
            "revieweeType": "PROVIDER",
            "careType": care_type,
            "pageSize": page_size
        }
        if page_token:
            variables["pageToken"] = page_token

        payload = {
            "query": REVIEWS_QUERY,
            "variables": variables
        }

        try:
            resp = self.session.post(GRAPHQL_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()

            # Sleep randomly to avoid detection or rate-limiting
            time.sleep(round(random.uniform(0.3, 0.65), 4))

            if "errors" in data:
                return None
            return data
        except Exception as e:
            self.logger.error(f"[ERROR] fetch_reviews_page caregiver={caregiver_id}, care_type={care_type}, pageToken={page_token}: {e}")
            return None

    def fetch_all_pages_of_reviews(
        self,
        caregiver_id: str,
        care_type: str,
        page_size: int = 10
    ):
        """
        Continuously fetch all pages of reviews for a caregiver & care_type,
        storing each page separately until no nextPageToken remains.
        """
        page_number = 1
        next_page_token = None

        while True:
            self.logger.info(
                f"Scraping caregiver={caregiver_id}, care_type={care_type}, page={page_number}, token={next_page_token or 'NONE'}"
            )

            data = self.fetch_reviews_page(
                caregiver_id=caregiver_id,
                care_type=care_type,
                page_size=page_size,
                page_token=next_page_token
            )

            file_path = build_review_path(self.postal_code, care_type, caregiver_id, page_number)

            if not data:
                self.logger.error(
                    f"Failed to get reviews for caregiver={caregiver_id}, care_type={care_type}, page={page_number}"
                )
                save_metadata(
                    directory=os.path.dirname(file_path),
                    caregiver_id=caregiver_id,
                    care_type=care_type,
                    page_number=page_number,
                    run_id=self.run_id,
                    status="error",
                    error="fetch_reviews_page returned None"
                )
                break  # stop pagination if we get an error

            # Save JSON
            try:
                save_json(file_path, data)
                self.logger.info(
                    f"Saved caregiver={caregiver_id} reviews for {care_type}, page={page_number} -> {file_path}"
                )
                save_metadata(
                    directory=os.path.dirname(file_path),
                    caregiver_id=caregiver_id,
                    care_type=care_type,
                    page_number=page_number,
                    run_id=self.run_id,
                    status="success"
                )
            except Exception as e:
                self.logger.error(f"Error saving caregiver={caregiver_id}, page={page_number}, care_type={care_type}: {e}")
                save_metadata(
                    directory=os.path.dirname(file_path),
                    caregiver_id=caregiver_id,
                    care_type=care_type,
                    page_number=page_number,
                    run_id=self.run_id,
                    status="error",
                    error=str(e)
                )
                break  # stop pagination if we fail to save

            # Check nextPageToken
            reviews_by_reviewee = data["data"]["reviewsByReviewee"]
            if reviews_by_reviewee["__typename"] == "ReviewsByRevieweePayload":
                next_page_token = reviews_by_reviewee["nextPageToken"]
                if not next_page_token:
                    # no more pages
                    break
            else:
                # failure or no next page
                break

            page_number += 1

    def scrape_reviews_for_caregiver(self, caregiver_id: str):
        """
        For one caregiver, request 3 care types: "CHILD_CARE", "SENIOR_CARE", "HOUSEKEEPING".
        Fetch multiple pages if nextPageToken is set.
        """
        CARE_TYPES = ["CHILD_CARE", "SENIOR_CARE", "HOUSEKEEPING"]

        for ctype in CARE_TYPES:
            self.fetch_all_pages_of_reviews(caregiver_id, ctype)

    def run_scrape(self):
        """
        Main method:
          1) Load caregiver IDs from 'raw_data/USA/<postal_code>/all_profiles/'
          2) For each caregiver, scrape 3 care types with pagination
          3) Save data + logs + metadata
        """
        caregiver_ids = self.load_caregiver_ids()
        self.logger.info(f"Starting review scrape for {len(caregiver_ids)} caregivers. run_id={self.run_id}")

        for cid in caregiver_ids:
            self.scrape_reviews_for_caregiver(cid)

        self.logger.info("Review scraping complete.")

# ------------------------------------------------------------------------------
# 4) EXAMPLE USAGE
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Provide the postal code you want to target
    postal_code = "07008"

    scraper = ReviewScraper(postal_code=postal_code)
    scraper.run_scrape()