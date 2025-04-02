"""
CaregiverProfileScraper:
Scrape Full Caregiver Profiles into a DataOps-Compliant Directory Structure

PURPOSE:
  1) Read caregiver IDs from existing "search" result files/directories.
  2) For each caregiver ID, retrieve the full profile via GraphQL.
  3) Save the resulting JSON to 'USA/all_profiles/<type>/<sub_type>/<profileID>.json'.
  4) Generate minimal metadata to track scraping status and potential errors.

DIRECTORY STRUCTURE:
  USA/
    all_profiles/
      <type>/          (e.g., 'childcare', 'housekeeping', 'seniorcare', etc.)
        <sub_type>/    (e.g., 'babysitting', 'nanny', 'inhome', etc.)
          <profileID>.json
          metadata_<profileID>.json  (optional if you want per-profile metadata)

DATAOPS FEATURES:
  - Logging to console for traceability
  - Random sleeps to avoid rate-limiting
  - Metadata creation for each profile or sub-directory if desired

DISCLAIMER:
  - For educational purposes; always respect Care.com’s Terms of Service.
  - Adjust code to your environment (paths, headers, error handling, etc.).
"""

import os
import json
import time
import random
import requests
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

# Replace or import your session cookie from config.py if needed
from config import COOKIE

# ------------------------------------------
# 1) HEADERS & GRAPHQL QUERIES
# ------------------------------------------

HEADERS = {
  'accept': '*/*',
  'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
  'apollographql-client-name': 'caregiver-profile-mfe',
  'apollographql-client-version': '1.179.0',
  'baggage': 'sentry-environment=prod,sentry-release=caregiver-profile-mfe%401.179.0,sentry-public_key=9e6efe115dbc42588d68792f5f100a34,sentry-trace_id=0115ccff564c487184d4b856dd174442,sentry-sample_rate=0.4,sentry-transaction=%2F%5Bvertical%5D%2F%5BcaregiverId%5D,sentry-sampled=false',
  'content-type': 'application/json',
  'origin': 'https://www.care.com',
  'priority': 'u=1, i',
  'referer': 'https://www.care.com/app/caregiver-profile/housekeeping/4a5bf001-ee05-42ab-8d19-60988ba8e8dd?zipcode=07008&radius=10&vertical=HOUSEKEEPING&subvertical=RECURRING&ageRanges=48%20-%2071%2C0%20-%2011%2C12%20-%2047%2C144%20-%20216%2C72%20-%20143&numberOfChildren=1&attributes=CPR_TRAINED&languages=ENGLISH&maxPayRate=49&minPayRate=15',
  'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'sentry-trace': '0115ccff564c487184d4b856dd174442-8ed1e1d5d025fa22-0',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    "Cookie": COOKIE
}

GRAPHQL_URL = "https://www.care.com/api/graphql"

GET_CAREGIVER_QUERY = """
query GetCaregiver($getCaregiverId: ID!, $serviceId: ServiceType, $shouldIncludeAllProfiles: Boolean, $shouldGetMarkedAsHired: Boolean!) {
  getCaregiver(
    id: $getCaregiverId
    serviceId: $serviceId
    shouldIncludeAllProfiles: $shouldIncludeAllProfiles
  ) {
    continuousBackgroundCheck {
      seeker {
        hasLimitReached
        subscriptionStatus
        __typename
      }
      hasActiveHit
      __typename
    }
    backgroundChecks {
      backgroundCheckName
      whenCompleted
      __typename
    }
    badges
    distanceFromSeekerInMiles
    educationDegrees {
      currentlyAttending
      degreeYear
      educationLevel
      schoolName
      educationDetailsText
      __typename
    }
    hasCareCheck
    hasGrantedCriminalBGCAccess
    hasGrantedMvrBGCAccess
    hasGrantedPremierBGCAccess
    hiredByCounts {
      locality {
        hiredCount
        __typename
      }
      __typename
    }
    hiredTimes
    isFavorite
    isMVREligible
    isVaccinated
    member {
      id
      lastName
      firstName
      gender
      hiResImageURL
      displayName
      email
      primaryService
      imageURL
      address {
        city
        state
        zip
        __typename
      }
      languages
      legacyId
      isPremium
      __typename
    }
    placeInfo {
      name
      __typename
    }
    profiles {
      serviceIds
      childCareCaregiverProfile {
        approvalStatus
        ageGroups
        availabilityFrequency
        bio {
          experienceSummary
          title
          aiAssistedBio
          __typename
        }
        childStaffRatio
        id
        maxAgeMonths
        minAgeMonths
        numberOfChildren
        offerings {
          ageRange {
            max {
              unit
              value
              __typename
            }
            min {
              unit
              value
              __typename
            }
            __typename
          }
          __typename
        }
        otherQualities
        payRange {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        qualities {
          afterSchoolCare
          babysitter
          certifiedNursingAssistant
          certifiedRegisterNurse
          certifiedTeacher
          childDevelopmentAssociate
          comfortableWithPets
          cprTrained
          crn
          doesNotSmoke
          doula
          earlyChildDevelopmentCoursework
          earlyChildhoodEducation
          firstAidTraining
          mothersHelper
          nafccCertified
          nanny
          newbornCareSpecialist
          nightNanny
          ownTransportation
          specialNeedsCare
          trustlineCertifiedCalifornia
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
        rates {
          hourlyRate {
            amount
            currencyCode
            __typename
          }
          numberOfChildren
          isDefaulted
          __typename
        }
        supportedServices {
          carpooling
          craftAssistance
          errands
          groceryShopping
          laundryAssistance
          lightHousekeeping
          mealPreparation
          swimmingSupervision
          travel
          __typename
        }
        yearsOfExperience
        __typename
      }
      commonCaregiverProfile {
        id
        repeatClientsCount
        merchandizedJobInterests {
          companionCare
          dateNight
          lightCleaning
          mealPrepLaundry
          mover
          personalAssistant
          petHelp
          shopping
          transportation
          __typename
        }
        __typename
      }
      petCareCaregiverProfile {
        approvalStatus
        rates {
          activity
          activityRate {
            amount
            currencyCode
            __typename
          }
          activityRateUnit
          __typename
        }
        bio {
          experienceSummary
          title
          __typename
        }
        id
        numberOfPetsComfortableWith
        otherQualities
        payRange {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        petSpecies {
          caresForAmphibians
          caresForBirds
          caresForCats
          caresForDogs
          caresForExoticPets
          caresForFarmAnimals
          caresForFish
          caresForHorses
          caresForMammals
          caresForOtherPets
          __typename
        }
        qualities {
          isBondedAndInsured
          isNappsCertified
          isPSAMemberAndIsInsured
          isRedCrossPetFirstAidCertified
          doesNotSmoke
          ownsTransportation
          isCertifiedAndInsured
          isExperiencedWithChallengingPets
          isExperiencedWithSeniorPets
          isExperiencedWithSpecialNeedsPets
          isExperiencedWithYoungPets
          __typename
        }
        recurringRate {
          hourlyRateFrom {
            currencyCode
            amount
            __typename
          }
          hourlyRateTo {
            currencyCode
            amount
            __typename
          }
          __typename
        }
        serviceRates {
          rate {
            amount
            currencyCode
            __typename
          }
          duration
          subtype
          __typename
        }
        supportedServices {
          administersMedicine
          boardsOvernight
          doesDailyFeeding
          doesHouseSitting
          doesPetDaycare
          doesPetSitting
          doesPetWalking
          groomsAnimals
          retrievesMail
          trainsDogs
          transportsPets
          watersPlants
          __typename
        }
        yearsOfExperience
        __typename
      }
      seniorCareCaregiverProfile {
        approvalStatus
        availabilityFrequency
        bio {
          experienceSummary
          title
          __typename
        }
        id
        otherQualities
        payRange {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        qualities {
          alzheimersOrDementiaExperience
          certifiedNursingAssistant
          comfortableWithPets
          cprTrained
          doesNotSmoke
          firstAidTraining
          homeHealthAideExperience
          hospiceExperience
          licensedNurse
          medicalEquipmentExperience
          ownTransportation
          registeredNurse
          woundCare
          __typename
        }
        recurringRate {
          hourlyRateFrom {
            currencyCode
            amount
            __typename
          }
          hourlyRateTo {
            currencyCode
            amount
            __typename
          }
          __typename
        }
        supportedServices {
          visitingPhysician
          visitingNurse
          transportation
          specializedCare
          specialNeeds
          respiteCare
          personalCare
          mobilityAssistance
          medicalTransportation
          medicalManagement
          mealPreparation
          lightHousekeeping
          liveInHomeCare
          hospiceServices
          homeModification
          homeHealth
          helpStayingPhysicallyActive
          heavyLifting
          feeding
          errands
          dementia
          companionship
          bathing
          __typename
        }
        yearsOfExperience
        __typename
      }
      tutoringCaregiverProfile {
        approvalStatus
        availabilityFrequency
        bio {
          experienceSummary
          title
          __typename
        }
        id
        otherGeneralSubjects
        otherQualities
        payRange {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        qualities {
          additionalDetails {
            doesNotSmoke
            isComfortableWithPets
            ownsTransportation
            __typename
          }
          professionalSkills {
            americanTutoringAssociationCertified
            certifiedTeacher
            __typename
          }
          __typename
        }
        supportedServices {
          tutorsInCenter
          tutorsInStudentsHome
          tutorsInTeachersHome
          tutorsOnline
          __typename
        }
        specificSubjects
        otherSpecificSubject
        yearsOfExperience
        __typename
      }
      houseKeepingCaregiverProfile {
        approvalStatus
        availabilityFrequency
        bio {
          experienceSummary
          title
          __typename
        }
        distanceWillingToTravel {
          unit
          value
          __typename
        }
        id
        otherQualities
        payRange {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        qualities {
          comfortableWithPets
          doesNotSmoke
          ownTransportation
          providesEquipment
          providesSupplies
          __typename
        }
        recurringRate {
          hourlyRateFrom {
            amount
            currencyCode
            __typename
          }
          hourlyRateTo {
            amount
            currencyCode
            __typename
          }
          __typename
        }
        schedule {
          endTime
          id
          ruleName
          rules
          startTime
          __typename
        }
        supportedServices {
          atticCleaning
          basementCleaning
          bathroomCleaning
          cabinetCleaning
          carpetCleaning
          changingBedLinens
          deepCleaning
          dishes
          dusting
          furnitureTreatment
          generalRoomCleaning
          houseSitting
          kitchenCleaning
          laundry
          moveOutCleaning
          organization
          ovenCleaning
          packingUnpacking
          petWasteCleanup
          plantCare
          refrigeratorCleaning
          standardCleaning
          surfacePolishing
          vacuumingOrMopping
          wallWashing
          windowWashing
          __typename
        }
        yearsOfExperience
        __typename
      }
      __typename
    }
    providerStatus
    recurringAvailability {
      dayList {
        monday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        tuesday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        wednesday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        thursday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        friday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        saturday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        sunday {
          blocks {
            end
            start
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    responseRate
    responseTime
    signUpDate
    yearsOfExperience
    nonPrimaryImages
    isMarkedAsHired @include(if: $shouldGetMarkedAsHired)
    __typename
  }
}
"""

# ------------------------------------------
# 2) HELPER FUNCTIONS
# ------------------------------------------

def load_search_results(root_dir: str) -> List[Dict[str, Any]]:
    """
    Recursively load all "search page" results from your existing directory structure,
    parse JSON files, and return them as a list of Python dicts.

    :param root_dir: The top-level directory where your initial search results are stored.
    :return: A list of parsed JSON objects from the search results.
    """
    results = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".json") and filename.startswith("page"): 
                # print(filename)
                # print(dirpath)
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    results.append(data)
                except Exception as e:
                    print(f"[WARNING] Could not parse {file_path}: {e}")
    return results

def extract_caregiver_ids(search_data: Dict[str, Any]) -> List[str]:
    """
    Given one search result JSON (a single file's data),
    extract all caregiver IDs. The exact path to IDs depends on your search response structure.

    :param search_data: Parsed JSON from a search result.
    :return: A list of caregiver IDs found in this search data.
    """
    caregiver_ids = []
    try:
        # Example path to edges -> node -> member -> id (adapt as needed)
        data_content = search_data["data"]
        search_providers_key = next((key for key in data_content if key.startswith("searchProviders")), None)
        edges = search_data["data"][search_providers_key]["searchProvidersConnection"]["edges"]
        for edge in edges:
            node = edge["node"]
            if node.get("__typename") == "Caregiver":
                caregiver_ids.append(node["member"]["id"])
    except Exception as e:
        print(f"[ERROR] extract_caregiver_ids: {e}")
    return caregiver_ids

def build_profile_path(postal_code:str ,profile_id: str, care_type: str, sub_type: str) -> str:
    """
    Create the path for storing a single caregiver's full profile:
      raw_data/USA/all_profiles/<care_type>/<sub_type>/<profileID>.json

    :param profile_id: The unique ID of the caregiver
    :param care_type: The main care type (e.g. 'childcare', 'housekeeping', etc.)
    :param sub_type: The sub-category (e.g. 'babysitting', 'nanny', 'inhome')
    :return: The absolute file path where the profile JSON should be stored
    """
    base_dir = os.path.join("raw_data","USA", postal_code, "all_profiles")
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, f"{profile_id}.json")
    return file_path

def save_metadata(file_path: str, meta: Dict[str, Any]) -> None:
    """
    Optionally save a small metadata JSON next to the profile, named metadata_<profileID>.json
    for a DataOps approach. This is not strictly required but can be useful for logging.

    :param file_path: The main profile file path, e.g. '.../<profileID>.json'
    :param meta: A dictionary of metadata (run times, errors, etc.)
    """
    directory = os.path.dirname(file_path)
    profile_id = os.path.splitext(os.path.basename(file_path))[0]
    metadata_path = os.path.join(directory, f"metadata_{profile_id}.json")

    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Failed to save metadata {metadata_path}: {e}")

# ------------------------------------------
# 3) MAIN SCRAPER LOGIC
# ------------------------------------------

class CaregiverProfileScraper:
    """
    A DataOps-oriented scraper that:
      1) Reads caretaker IDs from existing 'search' results
      2) Fetches full caretaker profiles from the GraphQL endpoint
      3) Saves them to 'USA/all_profiles/<type>/<sub_type>/<profileID>.json'
    """

    def __init__(
        self,
        postal_code: str,
        search_root: str = "raw_data",
        default_care_type: str = "childcare",
        default_sub_type: str = "babysitting"
    ):
        """
        :param search_root: Path to your previously scraped search results.
        :param default_care_type: Fallback type if you don't parse from the search data.
        :param default_sub_type: Fallback sub-type if not specified or unknown.
        """
        self.postal_code = postal_code
        self.search_root = search_root
        self.default_care_type = default_care_type
        self.default_sub_type = default_sub_type
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        # Each run gets a unique ID for logging / metadata
        self.run_id = str(uuid.uuid4())

    def fetch_caregiver_profile(
        self,
        caregiver_id: str,
        service_id: str = "HOUSEKEEPING",
        should_include_all_profiles: bool = True,
        should_get_marked_as_hired: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Fetches a single caregiver profile from the GraphQL endpoint.

        :param caregiver_id: The ID of the caregiver to retrieve
        :param service_id: The primary service context (CHILD_CARE, HOUSEKEEPING, etc.)
        :param should_include_all_profiles: If true, return all known profiles for the caregiver
        :param should_get_marked_as_hired: If true, include the 'isMarkedAsHired' field
        :return: Parsed JSON data for the caregiver, or None if error
        """
        
        
        payload = {
            "query": GET_CAREGIVER_QUERY,
            "variables": {
                "getCaregiverId": caregiver_id,
                "serviceId": service_id,
                "shouldIncludeAllProfiles": should_include_all_profiles,
                "shouldGetMarkedAsHired": should_get_marked_as_hired
            }
        }
        try:
            response = self.session.post(GRAPHQL_URL, json=payload)
            response.raise_for_status()
            data = response.json()

            # Sleep randomly to avoid detection or rate-limiting
            time.sleep(round(random.uniform(0.3, 0.7), 4))
            # Check for GraphQL-level errors
            if "errors" in data:
                print(f"[ERROR] GraphQL returned errors for {caregiver_id}: {data['errors']}")
                return None
            return data
        except Exception as e:
            print(f"[ERROR] fetch_caregiver_profile({caregiver_id}): {e}")
            return None

    def scrape_all_profiles(self):
        """
        Main pipeline:
          1) Load all search results from self.search_root
          2) Extract caregiver IDs
          3) For each ID, fetch the full profile
          4) Save under 'USA/all_profiles/<type>/<sub_type>/<profileID>.json'
        """
        print(f"[INFO] Starting caretaker profile scraping run_id={self.run_id}")

        # Step 1: Load all search results
        search_files_data = load_search_results(self.search_root)
        print(f"[INFO] Found {len(search_files_data)} search JSON files under '{self.search_root}'.")

        # Step 2: Extract caretaker IDs from each file
        all_caregiver_ids_list = []
        for file_data in search_files_data:
            caregiver_ids = extract_caregiver_ids(file_data)
            all_caregiver_ids_list.extend(caregiver_ids)

        print(f"[INFO] Extracted {len(all_caregiver_ids_list)} caregiver IDs before removing deduplication.")

        # Convert to set to remove duplicates
        all_caregiver_ids = set(all_caregiver_ids_list)

        print(f"[INFO] Extracted {len(all_caregiver_ids)} unique caregiver IDs after removing deduplication.")


        # Step 3: For each caretaker ID, fetch full profile
        for caretaker_id in all_caregiver_ids:
            # You might parse the care_type/sub_type from the search data or from the caretaker node.
            # Here we just use defaults, or you can define your own logic to guess type/subtype.
            care_type = self.default_care_type
            sub_type = self.default_sub_type

            # For example, if your search data was from housekeeping,
            # you might do care_type = "housekeeping" etc.
            # We'll do a basic approach:
            # service_id could match the folder or data we found them in
            service_id = "HOUSEKEEPING" if care_type == "housekeeping" else "CHILD_CARE"

            # Step 3.1: Fetch the full profile
            profile_data = self.fetch_caregiver_profile(
                caregiver_id = caretaker_id,
                service_id = service_id,
                should_include_all_profiles = True,
                should_get_marked_as_hired = False
            )
            if not profile_data:
                # We skip if there's an error
                continue

            # Step 3.2: Build the final path
            file_path = build_profile_path(self.postal_code, caretaker_id, care_type, sub_type)

            # Step 3.3: Save the JSON
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(profile_data, f, ensure_ascii=False, indent=2)
                print(f"[INFO] Saved caregiver '{caretaker_id}' to {file_path}")
            except Exception as e:
                print(f"[ERROR] Could not save profile {caretaker_id} to {file_path}: {e}")

            # Step 3.4: Optional - Save minimal metadata
            meta = {
                "run_id": self.run_id,
                "caretaker_id": caretaker_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "scrape_status": "success",
                "notes": "Full caretaker profile retrieval",
            }
            save_metadata(file_path, meta)

        print("[INFO] Finished scraping all caregiver profiles.")

# ------------------------------------------
# 4) EXAMPLE USAGE
# ------------------------------------------
if __name__ == "__main__":
    # Example instantiation
    scraper = CaregiverProfileScraper(
        postal_code="07008",           # the postal code of the search results
        search_root="raw_data",        # where your search JSON files are located
        default_care_type="childcare", # fallback if you can't parse from search data
        default_sub_type="babysitting" # fallback if you can't parse from search data
    )
    scraper.scrape_all_profiles()
