import requests
import json
from config import COOKIE

# Define the GraphQL API endpoint
GRAPHQL_URL = "https://www.care.com/api/graphql"

# Set up the request headers. The cookie is imported from config.py.
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ar;q=0.8",
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

# Define the GraphQL payload. The query is a multi-line string for clarity.
PAYLOAD = {
    "query": """
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
""",
    "variables": {
        "input": {
            "careType": "SITTER",
            "filters": {
                "postalCode": "07008",
                "searchPageSize": 10,
                "searchAfter": "U2VlZD0wLEluZGV4PTEwLFBhZ2VTY3JvbGxJZD0yNWZjNDJkNjgxMzA0NDRhOWVkZTY4OTcwNmM3YmY0NA=="
            },
            "agesServedInMonths": [0, 11, 12, 47, 48, 71, 72, 143, 144, 216],
            "attributes": [
                "CPR_TRAINED",
                "COMFORTABLE_WITH_PETS",
                "COLLEGE_EDUCATED",
                "OWN_TRANSPORTATION",
                "NON_SMOKER"
            ],
            "numberOfChildren": 1
        }
    }
}

def fetch_profiles():
    """
    Sends a POST request to the GraphQL endpoint with the defined payload and headers.
    Returns the JSON response.
    """
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json=PAYLOAD)
    response.raise_for_status()  # Raise an exception for HTTP errors.
    return response.json()

if __name__ == "__main__":
    try:
        result = fetch_profiles()
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {e}")
