import os
import time
import json
import requests
from config import COOKIE

# If you store the cookie in config.py, import it:
# from config import COOKIE

COOKIE = "YOUR_COOKIE_STRING"  # Replace if not using config.py

class CareScraper:
    """
    A pipeline to scrape babysitting (childcare) data from Care.com
    using a GraphQL endpoint, handling pagination and saving results.
    """

    GRAPHQL_URL = "https://www.care.com/api/graphql"

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
        "referer": (
            "https://www.care.com/app/search?"
            "zipcode=07008&radius=10&vertical=CHILD_CARE&subvertical=SITTER&"
            "ageRanges=48+-+71%2C0+-+11%2C12+-+47%2C144+-+216%2C72+-+143&"
            "numberOfChildren=1&attributes=CPR_TRAINED%2CCOMFORTABLE_WITH_PETS%2C"
            "COLLEGE_EDUCATED%2COWN_TRANSPORTATION%2CNON_SMOKER"
        ),
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
        "Cookie": COOKIE
    }

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

query SearchProvidersChildCare($input: SearchProvidersChildCareInput!) {
  searchProvidersChildCare(input: $input) {
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
        country_name: str,
        postal_code: str = "07008",
        care_type: str = "childcare",
        search_page_size: int = 10
    ):
        """
        :param country_name: The country where the search is performed (default "USA").
        :param postal_code: The ZIP/postal code for the search.
        :param care_type: Type of care to search for. Default is "childcare".
        :param search_page_size: How many profiles to fetch per page.
        """
        self.country_name = country_name
        self.postal_code = postal_code
        self.care_type = care_type  # e.g., "childcare", "sitter", etc.
        self.search_page_size = search_page_size

        # Build output directory structure:
        # raw_data/<country_name>/<postal_code>/search_of_<care_type>/
        self.output_dir = os.path.join(
            "raw_data",
            self.country_name,
            self.postal_code,
            f"search_of_{self.care_type.lower()}"
        )
        self._prepare_directories()

    def _prepare_directories(self):
        """Ensure that the necessary directories exist."""
        os.makedirs(self.output_dir, exist_ok=True)

    def _make_payload(self, search_after: str) -> dict:
        """
        Create the GraphQL payload for the request.
        :param search_after: The endCursor string from the previous page (Base64).
        :return: A dictionary to be sent as JSON in the request.
        """
        return {
            "query": self.QUERY,
            "variables": {
                "input": {
                    "careType": "SITTER",  # For babysitting specifically
                    "filters": {
                        "postalCode": self.postal_code,
                        "searchPageSize": self.search_page_size,
                        "searchAfter": search_after
                    },
                    "agesServedInMonths": [0, 11, 12, 47, 48, 71, 72, 143, 144, 216],
                    "numberOfChildren": 1
                }
            }
        }

    def fetch_all_profiles(self):
        """
        Fetches all caregiver profiles by continuously updating the
        'searchAfter' field until 'hasNextPage' is False.
        Saves each page of data as a separate JSON file in the output directory,
        printing progress at each step.
        """
        print(
            f"Starting scraping for country='{self.country_name}', "
            f"postal_code='{self.postal_code}', care_type='{self.care_type}' "
            f"with page size={self.search_page_size}..."
        )

        session = requests.Session()
        session.headers.update(self.HEADERS)

        has_next_page = True
        search_after = ""
        page_count = 0
        total_caregivers = 0  # We'll sum up edges across pages

        while has_next_page:
            page_count += 1
            payload = self._make_payload(search_after)
            response = session.post(self.GRAPHQL_URL, json=payload)
            response.raise_for_status()

            data = response.json()

            # Optional: check for GraphQL errors
            if "errors" in data:
                print(f"[ERROR] GraphQL errors on page {page_count}:", data["errors"])
                break

            child_care_data = data["data"]["searchProvidersChildCare"]

            # On the first page, we can display totalHits if present
            if page_count == 1 and "searchProvidersConnection" in child_care_data:
                total_hits = child_care_data["searchProvidersConnection"].get("totalHits")
                if total_hits is not None:
                    print(f"Total hits for this search: {total_hits}")

            connection = child_care_data["searchProvidersConnection"]
            edges = connection["edges"]
            num_edges = len(edges)
            total_caregivers += num_edges

            # Save the page data to a JSON file
            output_file = os.path.join(self.output_dir, f"page_{page_count}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            # Print progress info for this page
            print(
                f"Page {page_count} fetched: {num_edges} caregivers. "
                f"Data saved to '{output_file}'."
            )

            page_info = connection["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            search_after = page_info["endCursor"] if has_next_page else ""

            # If we have a next page, print the new endCursor
            if has_next_page:
                print(f"Next endCursor: {search_after}")

            # Sleep 1.5 seconds between requests
            time.sleep(1.5)

        print(
            f"Scraping complete. Pages fetched: {page_count}. "
            f"Total caregivers retrieved: {total_caregivers}."
        )

    def run(self):
        """
        Entry point for running the full scraping pipeline.
        """
        self.fetch_all_profiles()

if __name__ == "__main__":
    # Example usage: scrape babysitters in Carteret, NJ, USA
    scraper = CareScraper(country_name="USA", postal_code="07008", care_type="childcare")
    scraper.run()
