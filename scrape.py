import os
import time
import json
import requests
import random
from typing import Tuple, List, Optional
from config import COOKIE

# ------------------------------------------------------------------------------
# IMPORTANT:
#  1) Replace the COOKIE value with your actual session cookie or import it from
#     a config file (e.g., `from config import COOKIE`).
#  2) Make sure to respect the website's terms of service and scraping policies.
# ------------------------------------------------------------------------------

#COOKIE = "YOUR_COOKIE_STRING"  # Replace if not using an external config

class CareScraper:
    """
    A pipeline to scrape babysitting (childcare) data from Care.com using a
    GraphQL endpoint. This class implements a divide-and-conquer approach to
    segment the pay range so that each segment returns <= 500 caregivers.

    Directory structure for the output:
    raw_data/
        <country_name>/
            <postal_code>/
                search_of_<care_type>/
                    range_<minPay>_<maxPay>/
                        page_1.json
                        page_2.json
                        ...
    """

    # GraphQL endpoint for Care.com
    GRAPHQL_URL = "https://www.care.com/api/graphql"

    # Default HTTP headers (including the session cookie)
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

    # GraphQL query and fragment
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
        country_name: str = "USA",
        postal_code: str = "07008",
        care_type: str = "childcare",
        search_page_size: int = 10,
        min_pay_range: int = 10,
        max_pay_range: int = 50
    ):
        """
        Initialize the CareScraper with necessary parameters.

        :param country_name: The country where the search is performed (default "USA").
        :param postal_code: The ZIP/postal code for the search.
        :param care_type: Type of care to search for. Default is "childcare".
        :param search_page_size: How many profiles to fetch per page (default 10).
        :param min_pay_range: The lower bound of the pay range for the initial search.
        :param max_pay_range: The upper bound of the pay range for the initial search.
        """
        self.country_name = country_name
        self.postal_code = postal_code
        self.care_type = care_type
        self.search_page_size = search_page_size

        # The global min and max pay boundaries to be subdivided.
        self.min_pay_range = min_pay_range
        self.max_pay_range = max_pay_range

        # Base directory structure:
        # raw_data/<country_name>/<postal_code>/search_of_<care_type>/
        self.output_dir = os.path.join(
            "raw_data",
            self.country_name,
            self.postal_code,
            f"search_of_{self.care_type.lower()}"
        )
        os.makedirs(self.output_dir, exist_ok=True)

    def _make_payload(self, search_after: str, pay_min: int, pay_max: int) -> dict:
        """
        Create the GraphQL payload for a specific pay range and searchAfter cursor.

        :param search_after: The endCursor string from the previous page (Base64).
        :param pay_min: Minimum pay range boundary.
        :param pay_max: Maximum pay range boundary.
        :return: A dictionary to be sent as JSON in the request.
        """
        return {
            "query": self.QUERY,
            "variables": {
                "input": {
                    "careType": "SITTER",  # For babysitting specifically
                    "filters": {
                        # The dynamic payRange segment
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
                    },
                    "agesServedInMonths": [0, 11, 12, 47, 48, 71, 72, 143, 144, 216],
                    "numberOfChildren": 1
                }
            }
        }

    def _get_total_hits_for_range(self, pay_min: int, pay_max: int) -> int:
        """
        Perform a single request with a small page size to get totalHits for
        the given pay range. This is used to check if the result set is <= 500.

        :param pay_min: Minimum pay range boundary.
        :param pay_max: Maximum pay range boundary.
        :return: The total number of hits (caregivers) for this pay range.
        """
        # Use a small page size (e.g., 1) just to retrieve totalHits quickly
        temp_payload = dict(self._make_payload("", pay_min, pay_max))
        temp_payload["variables"]["input"]["filters"]["searchPageSize"] = 1

        response = requests.post(self.GRAPHQL_URL, headers=self.HEADERS, json=temp_payload)
        response.raise_for_status()
        data = response.json()

        # Sleep to avoid sending requests too quickly
        time.sleep(round(random.uniform(1.5, 1.8), 4))

        # Check for GraphQL errors
        if "errors" in data:
            print(f"[ERROR] GraphQL errors in _get_total_hits_for_range: {data['errors']}")
            # Return a large number so we consider subdividing again
            return 999999

        child_care_data = data["data"]["searchProvidersChildCare"]
        connection = child_care_data.get("searchProvidersConnection", {})
        total_hits = connection.get("totalHits", 0)

        return total_hits

    def _fetch_profiles_for_range(self, pay_min: int, pay_max: int):
        """
        Fetch all caregiver profiles within a specific pay range by paginating
        until hasNextPage is False. Each page is saved to a JSON file.

        :param pay_min: Minimum pay range boundary.
        :param pay_max: Maximum pay range boundary.
        """
        # Create a subdirectory for this specific range
        range_dir = os.path.join(self.output_dir, f"range_{pay_min}_{pay_max}")
        os.makedirs(range_dir, exist_ok=True)

        session = requests.Session()
        session.headers.update(self.HEADERS)

        has_next_page = True
        search_after = ""
        page_count = 0
        total_caregivers_in_range = 0

        print(f"  -> Fetching data for pay range [{pay_min}, {pay_max}]...")

        while has_next_page:
            page_count += 1
            payload = self._make_payload(search_after, pay_min, pay_max)
            response = session.post(self.GRAPHQL_URL, json=payload)
            response.raise_for_status()
            data = response.json()

            # Sleep 1.5 seconds between requests
            time.sleep(1.5)

            # Check for GraphQL errors
            if "errors" in data:
                print(f"[ERROR] GraphQL errors on page {page_count} for range [{pay_min}, {pay_max}]:")
                print(data["errors"])
                break

            child_care_data = data["data"]["searchProvidersChildCare"]
            connection = child_care_data.get("searchProvidersConnection", {})
            edges = connection.get("edges", [])
            page_info = connection.get("pageInfo", {})

            # Count how many caregivers we got on this page
            num_edges = len(edges)
            total_caregivers_in_range += num_edges

            # Save page data
            output_file = os.path.join(range_dir, f"page_{page_count}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print(f"    - Page {page_count} => {num_edges} caregivers, saved to {output_file}")

            # Update pagination
            has_next_page = page_info.get("hasNextPage", False)
            search_after = page_info.get("endCursor", "")

            if has_next_page:
                print(f"    - Next endCursor: {search_after}")

        print(f"  -> Finished range [{pay_min}, {pay_max}]. Total pages: {page_count}, total caregivers: {total_caregivers_in_range}.\n")

    def run(self):
        """
        Main entry point that uses a divide-and-conquer (binary search-like) strategy
        to split the pay range into segments, each returning <= 500 caregivers.

        1. Initialize a queue with the global min/max pay range.
        2. Pop a range from the queue and get totalHits.
        3. If totalHits > 500, split the range into two subranges and re-check them.
        4. If totalHits <= 500, fetch all profiles for that range via pagination.
        5. Repeat until all subranges are processed.
        """
        print(f"Starting scraping with pay range [{self.min_pay_range}, {self.max_pay_range}] for {self.care_type} in {self.postal_code}, {self.country_name}.\n")

        # Queue of tuples (pay_min, pay_max)
        ranges_to_check: List[Tuple[int, int]] = [(self.min_pay_range, self.max_pay_range)]

        while ranges_to_check:
            pay_min, pay_max = ranges_to_check.pop(0)

            print(f"Checking pay range [{pay_min}, {pay_max}]...")
            total_hits = self._get_total_hits_for_range(pay_min, pay_max)
            print(f"  => totalHits = {total_hits}")

            if total_hits > 500:
                # Need to subdivide further
                if pay_min == pay_max:
                    # Can't split further if they're the same
                    print(f"  [WARNING] Range [{pay_min}, {pay_max}] is a single value but hits > 500. Skipping or you can handle differently.\n")
                    continue

                # Split the range roughly in half
                mid = (pay_min + pay_max) // 2

                # Enqueue the two new subranges
                left_range = (pay_min, mid)
                right_range = (mid + 1, pay_max)

                print(f"  => Splitting into [{left_range[0]}, {left_range[1]}] and [{right_range[0]}, {right_range[1]}].\n")
                ranges_to_check.append(left_range)
                ranges_to_check.append(right_range)

            else:
                # totalHits <= 500, so we can safely paginate
                self._fetch_profiles_for_range(pay_min, pay_max)

        print("All pay range segments processed. Scraping complete!")

# ------------------------------------------------------------------------------
# Example usage
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create an instance of CareScraper. For example:
    #   country_name = "USA"
    #   postal_code = "07008"
    #   care_type = "childcare"
    #   search_page_size = 10
    #   min_pay_range = 15
    #   max_pay_range = 50
    #
    # Adjust these values as needed:
    scraper = CareScraper(
        country_name="USA",
        postal_code="07008",
        care_type="childcare",
        search_page_size=10,
        min_pay_range=15,
        max_pay_range=50
    )

    # Run the divide-and-conquer scraping process
    scraper.run()
