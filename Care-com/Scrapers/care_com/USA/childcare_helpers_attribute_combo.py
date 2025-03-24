# helpers_attribute_combo.py

import itertools
import threading
import concurrent.futures
import time
import random
import json
import os
from typing import List, Set, Tuple

def generate_attribute_combinations(base_attributes: List[str]) -> List[List[str]]:
    """
    Generate all subsets (combinations) of the given list of attributes.
    Excludes the empty set if desired (or include it if you prefer).
    """
    all_combos = []
    # Add an empty subset to account for the case where no attributes are selected.
    all_combos.append([])
    # range(1, len(...) + 1) => skip empty combination
    for r in range(1, len(base_attributes) + 1):
        for combo in itertools.combinations(base_attributes, r):
            all_combos.append(list(combo))
    return all_combos

def scrape_ids_for_range_with_attributes(
    session,
    logger,
    wraper_provider_key: List[str],
    care_type: str,
    thread_lock: threading.Lock,
    pay_min: int,
    pay_max: int,
    attributes_subset: List[str],
    postal_code: str,
    base_query: str,
    base_headers: dict,
    base_url: str,
    sort_order: str = "SORT_ORDER_REVIEW_RATING_ASCENDING",
) -> Set[str]:
    """
    Perform a segmented scraping for the given pay range (pay_min, pay_max),
    using the specified subset of attributes in the GraphQL variables.
    - session: requests.Session() to reuse cookies, headers
    - logger: for logging info or errors
    - thread_lock: a Lock for merging sets in a thread-safe way
    - pay_min, pay_max: single-value pay range
    - attributes_subset: e.g. ["CPR_TRAINED", "NON_SMOKER", ...]
    - postal_code: e.g. "10001"
    - base_query: The GraphQL query string
    - base_headers: The standard headers (including Cookie)
    - base_url: The GraphQL endpoint
    Returns a set of caregiver IDs found for this attribute combo.
    """
    collected_ids = set()
    search_after = ""
    page_count = 1

    while True:
        if len(attributes_subset) > 0:
            payload = {
                "query": base_query,
                "variables": {
                    "input": {
                        "careType": care_type,
                        "filters": {
                            "payRange": {
                                "min": {"amount": pay_min, "currencyCode": "USD"},
                                "max": {"amount": pay_max, "currencyCode": "USD"}
                            },
                            "postalCode": postal_code,
                            "searchPageSize": 10,
                            "searchAfter": search_after,
                            "languagesSpoken": ["ENGLISH"],
                            "searchSortOrder": sort_order
                        },
                        "attributes": attributes_subset,
                        "agesServedInMonths": [0, 11, 12, 47, 48, 71, 72, 143, 144, 216],
                        "numberOfChildren": 1
                    }
                }
            }
        else:
            payload = {
                "query": base_query,
                "variables": {
                    "input": {
                        "careType": care_type,
                        "filters": {
                            "payRange": {
                                "min": {"amount": pay_min, "currencyCode": "USD"},
                                "max": {"amount": pay_max, "currencyCode": "USD"}
                            },
                            "postalCode": postal_code,
                            "searchPageSize": 10,
                            "searchAfter": search_after,
                            "languagesSpoken": ["ENGLISH"],
                            "searchSortOrder": sort_order
                        },
                        "agesServedInMonths": [0, 11, 12, 47, 48, 71, 72, 143, 144, 216],
                        "numberOfChildren": 1
                    }
                }
            }

        try:
            resp = session.post(base_url, json=payload, headers=base_headers)
            resp.raise_for_status()
            #data = resp.json()
            
            try:
                data = resp.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON: {e}")
                logger.error(f"Raw response text: {resp.text}")
                return

            # If "data" not in data or the structure is unexpected
            if "data" not in data:
                logger.error(f"Response missing 'data': {resp.text}")
                return

            # If "errors" in data
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return
            

            # Sleep randomly
            time.sleep(round(random.uniform(.35, 0.55), 4))
            #logger.info(f"Sleeping")

            data_content = data["data"]
            search_providers_key = next((key for key in data_content if key.startswith("searchProviders")), None)
            edges = data["data"][search_providers_key]["searchProvidersConnection"]["edges"]
            #edges = data["data"]["searchProvidersChildCare"]["searchProvidersConnection"]["edges"]
            for edge in edges:
                node = edge["node"]
                if node.get("__typename") == "Caregiver":
                    caregiver_id = node["member"]["id"]
                    collected_ids.add(caregiver_id)
            #logger.info(f"Collected IDS from page: {page_count}")
            
            page_info = data["data"][search_providers_key]["searchProvidersConnection"]["pageInfo"]
            if page_info["hasNextPage"]:
                search_after = page_info["endCursor"]
                page_count += 1
            else:
                break

        except Exception as e:
            logger.error(f"[Thread Error] pay_range=[{pay_min}, {pay_max}], combo={attributes_subset}, page={page_count}: {e}")
            break

    # Thread-safe merging of results
    with thread_lock:
        if search_providers_key == ' ':
            wraper_provider_key[0] = "searchProvidersSeniorCare"
        else:
            wraper_provider_key[0] = "searchProvidersSeniorCare"# search_providers_key    
        return collected_ids

def run_threaded_combinations(
    session,
    logger,
    wraper_provider_key: List[str],
    care_type: str,
    pay_min: int,
    pay_max: int,
    total_hits: int,
    base_attributes: List[str],
    postal_code: str,
    base_query: str,
    base_headers: dict,
    base_url: str
) -> Set[str]:
    """
    1) Generate all attribute combos.
    2) Spawn threads to scrape IDs for each combo in parallel.
    3) Aggregate all unique IDs into a global set.
    4) Compare final count with totalHits, log the difference.
    5) Return the final aggregated set of caregiver IDs.
    """
    logger.info(f"Single-value payrange=[{pay_min}, {pay_max}] has >500 hits => run attribute combos...")

    combos = generate_attribute_combinations(base_attributes)
    logger.info(f"Generated {len(combos)} attribute combinations.")

    aggregated_ids = set()
    thread_lock = threading.Lock()

    def worker(combo: List[str]) -> None:
        #check if necessary to scrape:
        with thread_lock:
            if len(aggregated_ids) >= total_hits :return None #len(aggregated_ids) >= total_hits: return
        combo_ids = scrape_ids_for_range_with_attributes(
            session=session,
            logger=logger,
            wraper_provider_key=wraper_provider_key,
            care_type=care_type,
            thread_lock=thread_lock,
            pay_min=pay_min,
            pay_max=pay_max,
            attributes_subset=combo,
            postal_code=postal_code,
            base_query=base_query,
            base_headers=base_headers,
            base_url=base_url
        )
        if len(combo_ids) > 498:
            logger.info(f"Doing reverse sorting (SORT_ORDER_REVIEW_RATING_DESCENDING) on Combo: {combo}...")
            combo_ids2 =  scrape_ids_for_range_with_attributes(
            session=session,
            logger=logger,
            wraper_provider_key=wraper_provider_key,
            care_type=care_type,
            thread_lock=thread_lock,
            pay_min=pay_min,
            pay_max=pay_max,
            attributes_subset=combo,
            postal_code=postal_code,
            base_query=base_query,
            base_headers=base_headers,
            base_url=base_url,
            sort_order="SORT_ORDER_REVIEW_RATING_DESCENDING"
            )
            logger.info(f"doing SORT_ORDER_RECOMMENDED_DESCENDING on Combo: {combo}...")
            combo_ids3 =  scrape_ids_for_range_with_attributes(
            session=session,
            logger=logger,
            wraper_provider_key=wraper_provider_key,
            care_type=care_type,
            thread_lock=thread_lock,
            pay_min=pay_min,
            pay_max=pay_max,
            attributes_subset=combo,
            postal_code=postal_code,
            base_query=base_query,
            base_headers=base_headers,
            base_url=base_url,
            sort_order="SORT_ORDER_RECOMMENDED_DESCENDING"
            )
            logger.info(f"doing SORT_ORDER_DISTANCE_ASCENDING on Combo: {combo}...")
            combo_ids4 =  scrape_ids_for_range_with_attributes(
            session=session,
            logger=logger,
            wraper_provider_key=wraper_provider_key,
            care_type=care_type,
            thread_lock=thread_lock,
            pay_min=pay_min,
            pay_max=pay_max,
            attributes_subset=combo,
            postal_code=postal_code,
            base_query=base_query,
            base_headers=base_headers,
            base_url=base_url,
            sort_order="SORT_ORDER_DISTANCE_ASCENDING"
            )
        if combo_ids:
            # Merge in a thread-safe manner
            with thread_lock:
                aggregated_ids.update(combo_ids)
                if combo_ids2: aggregated_ids.update(combo_ids2)
                if combo_ids3: aggregated_ids.update(combo_ids3)
                if combo_ids4: aggregated_ids.update(combo_ids4)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(worker, c) for c in combos]
        concurrent.futures.wait(futures)

    unique_count = len(aggregated_ids)
    logger.info(f"Aggregated {unique_count} unique caregiver IDs for payrange=[{pay_min}, {pay_max}].")
    logger.info(f"Original totalHits from get_total_hits_for_range = {total_hits}.")
    
    return aggregated_ids

def create_aggregated_search_file(
    logger,
    base_dir: str,
    wraper_provider_key: List[str],
    range_total_hits: int,
    pay_min: int,
    pay_max: int,
    caregiver_ids: Set[str],
    unique_count: int
) -> str:
    """
    Build a final JSON file that mimics your typical "page_<>.json" structure,
    but containing the aggregated caregiver IDs. Save in range_{pay_min}_{pay_max}/
    e.g. "page_attributes.json" or "page_1.json"
    """
    range_dir = os.path.join(base_dir, f"range_{pay_min}_{pay_max}")
    os.makedirs(range_dir, exist_ok=True)

    result_data = {
        "data": {
            wraper_provider_key[0]: {
                "searchProvidersConnection": {
                    "edges": []
                },
                "aggregatedUniqueCount": unique_count,
                "RangeTotalHits": range_total_hits
            }
        }
    }

    edges_list = []
    for cid in caregiver_ids:
        edge_item = {
            "node": {
                "__typename": "Caregiver",
                "member": {
                    "id": cid
                }
            }
        }
        edges_list.append(edge_item)
    data_content = result_data["data"]
    search_providers_key = next((key for key in data_content if key.startswith("searchProviders")), None)
    result_data["data"][search_providers_key]["searchProvidersConnection"]["edges"] = edges_list

    file_path = os.path.join(range_dir, "page_attributes.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(result_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Created aggregated page file: {file_path}")
    return file_path
