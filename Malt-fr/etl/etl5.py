"""
Malt Data Processor (ETL): Raw to Structured CSV & Custom Flat File

PURPOSE:
  1) This script performs an ETL process on the raw data scraped from Malt.fr,
     first creating a set of 11 normalized CSV files.
  2) Following the initial ETL, it runs a second process to generate a denormalized,
     "flat file" (`custom_profile.csv`) specifically designed for statistical
     analysis in tools like R.
  3) It pivots one-to-many relationships (skills, experiences) into wide-format
     columns (e.g., Compétence 1, Compétence 2).
  4) It performs feature engineering to create new analytical variables, such as
     character counts and categorical mappings.
  5) This script follows DataOps principles by creating run-specific output folders
     and generating metadata for data lineage and processing transparency.

VERSION: 3.1.0 (Added custom_profile.csv generation for statistical analysis)

PREREQUISITES:
  - `pip install pandas lxml tqdm`

USAGE:
  - This script is designed to be run from a Jupyter Notebook or another Python script.
  - Call the main function `process_malt_data()`. The custom CSV will be generated
    automatically at the end.

Author: @AyoubFrihaoui
Version: 3.1.0
"""

import os
import re
import json
import logging
import time
import pandas as pd
from lxml import html
from tqdm import tqdm
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone


# --- Configuration ---
class Config:
    RAW_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\raw_data"
    HTML_ROOT = r"H:\Data Extraction FSP\Malt-fr\raw_data\malt_fr\developpeur\en_télétravail\profiles\developpeur\en_télétravail\profiles"
    PROCESSED_DATA_ROOT = r"D:\Data Extraction FSP\Malt-fr\processed_data"
    TABLE_NAMES = [
        "profiles",
        "profile_skills",
        "profile_badges",
        "profile_languages",
        "profile_categories",
        "portfolio_items",
        "reviews",
        "recommendations",
        "experiences",
        "education",
        "certifications",
    ]


# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- Helper & Parsing Functions (Unchanged from v3.0) ---
def safe_get(data: Dict, keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


def find_and_load_json_files(root_path: str) -> List[str]:
    json_files = []
    logger.info(f"Searching for raw JSON files in: {root_path}")
    for dirpath, _, filenames in os.walk(root_path):
        for filename in filenames:
            if filename.startswith("page_") and filename.endswith(".json"):
                json_files.append(os.path.join(dirpath, filename))
    logger.info(f"Found {len(json_files)} raw JSON files.")
    return json_files


def extract_profiles_from_json(json_files: List[str]) -> Dict[str, Dict]:
    profiles = {}
    logger.info("Extracting unique profiles from JSON files...")
    for file_path in tqdm(json_files, desc="Reading JSON files"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for profile_data in data.get("profiles", []):
                profile_id = profile_data.get("id")
                if profile_id and profile_id not in profiles:
                    profiles[profile_id] = profile_data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read or parse {file_path}: {e}")
    logger.info(f"Extracted {len(profiles)} unique profiles.")
    return profiles


# --- HTML Parsing Functions ---
def parse_ld_json(html_content: str) -> Optional[Dict]:
    """Parses HTML to find and load the second 'application/ld+json' script."""
    ld_json_scripts = re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html_content, re.DOTALL
    )
    if len(ld_json_scripts) > 1:
        try:
            return json.loads(ld_json_scripts[1])
        except json.JSONDecodeError:
            return None
    return None


def extract_with_xpath(tree: html.HtmlElement, xpath: str) -> Optional[str]:
    if not xpath:
        return None
    try:
        elements = tree.xpath(xpath)
        if elements:
            return elements[0].text_content().strip()
    except Exception as e:
        logger.debug(f"XPath evaluation failed for '{xpath}': {e}")
    return None


def extract_list_with_xpath(
    tree: html.HtmlElement, xpath_template: str, limit=150
) -> List[str]:
    items = []
    for index in range(1, limit + 1):
        xpath = xpath_template.format(index=index)
        element_text = extract_with_xpath(tree, xpath)
        if element_text:
            items.append(element_text)
        else:
            break
    return items


def extract_nested_list_with_xpath(
    tree: html.HtmlElement, xpath_template: str, limit=10
) -> List[str]:
    items = []
    for outer_index in range(1, limit + 1):
        found_in_outer = False
        for inner_index in range(1, limit + 1):
            xpath = xpath_template.format(index_cat=outer_index, index_item=inner_index)
            element_text = extract_with_xpath(tree, xpath)
            if element_text:
                items.append(element_text)
                found_in_outer = True
            else:
                break
        if not found_in_outer:
            break
    return items


# --- Main Processor Class (Unchanged from v3.0) ---
class MaltDataProcessor:
    def __init__(self, output_run_path: str):
        self.data_frames: Dict[str, List[Dict]] = {
            name: [] for name in Config.TABLE_NAMES
        }
        self.retrieved_at = datetime.now(timezone.utc).isoformat()
        self.output_path = output_run_path
        self.processed_count = 0
        self.error_count = 0

    def process_profile(self, profile_id: str, json_data: Dict):
        """Processes a single profile, combining data from JSON and its corresponding HTML file."""
        html_file_path = os.path.join(Config.HTML_ROOT, f"{profile_id}.html")
        html_content = None
        ld_json_data = {}
        tree = None
        if os.path.exists(html_file_path):
            try:
                with open(html_file_path, "r", encoding="utf-8") as f:
                    html_content = f.read()
                tree = html.fromstring(html_content)
                parsed_ld = parse_ld_json(html_content)
                if parsed_ld:
                    ld_json_data = parsed_ld
            except (IOError, html.etree.ParserError) as e:
                logger.warning(
                    f"Could not read or parse HTML for profile {profile_id}: {e}"
                )
                self.error_count += 1
                return
        else:
            logger.debug(
                f"HTML file not found for profile {profile_id}. Proceeding with JSON data only."
            )

        # --- 1. PROFILES TABLE ---
        full_name = safe_get(ld_json_data, ["name"], "")
        first_name = json_data.get("firstName", "")
        last_name = (
            full_name.replace(first_name, "").strip()
            if full_name and first_name
            else None
        )
        profile_row = {
            "profile_id": profile_id,
            "first_name": first_name,
            "last_name": last_name,
            "headline": json_data.get("headline"),
            "city": safe_get(json_data, ["location", "city"]),
            "location_type": safe_get(json_data, ["location", "locationType"]),
            "price_amount": safe_get(json_data, ["price", "value", "amount"]),
            "price_currency": safe_get(json_data, ["price", "value", "currency"]),
            "profile_url": safe_get(ld_json_data, ["url"]),
            "photo_url": safe_get(ld_json_data, ["image"]),
            "availability_status": safe_get(json_data, ["availability", "status"]),
            "work_availability": safe_get(
                json_data, ["availability", "workAvailability"]
            ),
            "next_availability_date": safe_get(
                json_data, ["availability", "nextAvailabilityDate"]
            ),
            "stats_missions_count": safe_get(json_data, ["stats", "missionsCount"]),
            "stats_recommendations_count": safe_get(
                json_data, ["stats", "recommendationsCount"]
            ),
            "aggregate_rating_value": (
                float(re.search(r'"ratingValue"\s*:\s*([\d.]+)', html_content).group(1))
                if re.search(r'"ratingValue"\s*:\s*([\d.]+)', html_content)
                else None
            ),
            "aggregate_rating_count": (
                int(re.search(r'"ratingCount"\s*:\s*(\d+)', html_content).group(1))
                if re.search(r'"ratingCount"\s*:\s*(\d+)', html_content)
                else None
            ),
            "aggregate_review_count": (
                int(re.search(r'"reviewCount"\s*:\s*(\d+)', html_content).group(1))
                if re.search(r'"reviewCount"\s*:\s*(\d+)', html_content)
                else None
            ),
            "aggregate_best_rating": (
                int(re.search(r'"bestRating"\s*:\s*(\d+)', html_content).group(1))
                if re.search(r'"bestRating"\s*:\s*(\d+)', html_content)
                else None
            ),
            # "aggregate_worst_rating": safe_get(ld_json_data, ['aggregateRating', 'worstRating']),
            "aggregate_worst_rating": (
                int(re.search(r'"worstRating"\s*:\s*(\d+)', html_content).group(1))
                if re.search(r'"worstRating"\s*:\s*(\d+)', html_content)
                else None
            ),
            "retrieved_at": self.retrieved_at,
        }
        # right after you read `html_content` and build `tree`:
        snippet = html_content[
            html_content.find("profile-section-description__content")
            - 200 : html_content.find("profile-section-description__content")
            + 200
        ]
        print("--- HTML SNIPPET AROUND DESCRIPTION ---")
        print(snippet)
        print("--------------------------------------")
        if tree is not None:
            profile_row.update(
                {
                    "experience_years_text": extract_with_xpath(
                        tree,
                        '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[2]/span[2]',
                    ),
                    "response_rate_text": extract_with_xpath(
                        tree,
                        '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[3]/span[2]',
                    ),
                    "response_time_text": extract_with_xpath(
                        tree,
                        '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[1]/div/div/div[3]/ul/li[4]/span[2]',
                    ),
                    "profile_description": extract_with_xpath(
                        tree, '//*[@data-testid="profile-description"]/div'
                    ),
                    "has_signed_charter": bool(
                        extract_with_xpath(
                            tree,
                            '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[1]/div/div[2]/p',
                        )
                    ),
                    "has_verified_email": bool(
                        extract_with_xpath(
                            tree,
                            '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[2]/div/div/div[4]/div/div[2]',
                        )
                    ),
                }
            )
        self.data_frames["profiles"].append(profile_row)

        for skill in json_data.get("skills", []):
            self.data_frames["profile_skills"].append(
                {
                    "profile_id": profile_id,
                    "skill_name": skill.get("label"),
                    "is_certified": skill.get("certified"),
                }
            )
        for badge in json_data.get("badges", []):
            self.data_frames["profile_badges"].append(
                {"profile_id": profile_id, "badge_name": badge}
            )
        for item in json_data.get("portfolio", []):
            self.data_frames["portfolio_items"].append(
                {
                    "profile_id": profile_id,
                    "item_index": item.get("index"),
                    "title": item.get("title"),
                    "item_type": item.get("type"),
                    "low_res_url": safe_get(item, ["picture", "lowResolutionUrl"]),
                    "med_res_url": safe_get(item, ["picture", "mediumResolutionUrl"]),
                    "high_res_url": safe_get(item, ["picture", "highResolutionUrl"]),
                }
            )

        if tree is not None:
            for lang in extract_list_with_xpath(
                tree, '//*[@id="languages-section"]/div/ul/li[{index}]/p[1]'
            ):
                self.data_frames["profile_languages"].append(
                    {"profile_id": profile_id, "language_name": lang}
                )
            for cat in extract_nested_list_with_xpath(
                tree,
                '//*[@id="__nuxt"]/div/main/div/div[1]/div[2]/div/div[3]/div[1]/div/section[4]/div/div[{index_cat}]/ul/li[{index_item}]/a/span',
            ):
                self.data_frames["profile_categories"].append(
                    {"profile_id": profile_id, "category_name": cat}
                )
            for index in range(3, 151):  # Recommendations start from index 3
                base_xpath = f'//*[@data-testid="read-more-recommendations-content"]/div[{index}]'
                if not tree.xpath(base_xpath):
                    break
                name = (
                    # extract_list_with_xpath(
                    #     tree, f"{base_xpath}"
                    #           "//span["
                    #             "contains(concat(' ', normalize-space(@class), ' '), ' author-name ')"
                    #             " or "
                    #             "contains(concat(' ', normalize-space(@class), ' '), ' recommendation__header__author__name ')"
                    #           "][1][1]" #[1]/text()"
                    # )
                    # extract_with_xpath(
                    #     tree, f'/html/body/div[1]/div/main/div/div[1]/div[2]/div/div[3]/div[2]/div/section[6]/div/div/div[{index}]/div/div/div[2]/div[1]/div[1]/span[2]'
                    # )
                #     or extract_with_xpath(
                #         tree,
                #         f"{base_xpath}/div/div/div[2]/div[1]/div[1]/a/span/span[2]",
                #     )
                     extract_with_xpath(
                        tree,
                        f'//*[@data-testid="read-more-recommendations-content"]/div[{index}]//span[contains(concat(" ", normalize-space(@class), " "), " author-name ")]',
                    )
                #     or extract_with_xpath(
                #         tree,
                #         f'{base_xpath}//*[@data-testid="read-more-recommendations-content"]/div[5]/div//span[contains(concat(" ", normalize-space(@class), " "), " recommendation__header__author__name ")]',
                #     )
                 )
                self.data_frames["recommendations"].append(
                    {
                        "profile_id": profile_id,
                        "recommendation_index": index - 1,
                        "recommender_name": name,
                        "recommender_company": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[2]/div[1]/div[2]"
                        ),
                        "recommendation_date": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[2]/div[2]"
                        ),
                        "recommendation_desc": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[2]/div[3]"
                        ),
                    }
                )
            for index in range(1, 151):
                base_xpath = f'//*[@class="profile-experiences__list"]/li[{index}]'
                if not tree.xpath(base_xpath):
                    break
                self.data_frames["experiences"].append(
                    {
                        "profile_id": profile_id,
                        "experience_index": index - 1,
                        "company_name": extract_with_xpath(
                            tree, f"{base_xpath}/div/div[2]/div/div[1]/div"
                        ),
                        "title": extract_with_xpath(
                            tree, f"{base_xpath}/div/div[2]/div/div[2]"
                        ),
                        "date_range": extract_with_xpath(
                            tree, f"{base_xpath}/div/div[2]/div/div[3]/div[1]/span"
                        ),
                        "location": extract_with_xpath(
                            tree, f"{base_xpath}/div/div[2]/div/div[3]/div[2]"
                        ),
                        "description": extract_with_xpath(
                            tree, f"{base_xpath}/div/div[2]/div/div[4]/div"
                        ),
                    }
                )
            for index in range(1, 151):
                base_xpath = f'//*[@class="profile-educations__list"]/li[{index}]'
                if not tree.xpath(base_xpath):
                    break
                self.data_frames["education"].append(
                    {
                        "profile_id": profile_id,
                        "education_index": index - 1,
                        "degree": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div/div[1]/span"
                        ),
                        "institution": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div/div[2]"
                        ),
                        "date": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div/div[3]/small"
                        ),
                    }
                )
            for index in range(1, 151):
                base_xpath = f'//*[@class="profile-certifications__list"]/li[{index}]'
                if not tree.xpath(base_xpath):
                    break
                self.data_frames["certifications"].append(
                    {
                        "profile_id": profile_id,
                        "certification_index": index - 1,
                        "name": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[1]"
                        ),
                        "institution": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[2]"
                        ),
                        "link": extract_with_xpath(
                            tree, f"{base_xpath}/div/div/div[4]/a/span"
                        ),
                    }
                )
            for index in range(2, 151):  # Reviews start from index 2
                base_xpath = f'//*[@class="profile-appraisal"][{index}]/article'
                if not tree.xpath(base_xpath):
                    break
                self.data_frames["reviews"].append(
                    {
                        "profile_id": profile_id,
                        "review_index": index - 1,
                        "name": extract_with_xpath(
                            tree, f"{base_xpath}/div[2]/div/div/h2/span[1]"
                        ),
                        "company": extract_with_xpath(
                            tree, f"{base_xpath}/div[2]/div/div/h2/span[3]"
                        ),
                        "date": extract_with_xpath(
                            tree, f"{base_xpath}/div[2]/div/div/p"
                        ),
                        "description": extract_with_xpath(
                            tree, f"{base_xpath}/div[2]/p"
                        ),
                    }
                )
        self.processed_count += 1

    def save_to_csv(self):
        logger.info(f"Saving processed data to: {self.output_path}")
        for name in Config.TABLE_NAMES:
            data = self.data_frames.get(name, [])
            if data:
                df = pd.DataFrame(data)
                output_file = os.path.join(self.output_path, f"{name}.csv")
                df.to_csv(output_file, index=False, encoding="utf-8-sig")
                logger.info(f"  - Saved {len(df)} rows to {name}.csv")
            else:
                logger.warning(
                    f"  - No data found for '{name}', CSV file will not be created."
                )


# --- NEW: Custom Profile View Generation ---
def create_custom_profile_view(run_path: str):
    """
    Creates a single, wide-format CSV file for statistical analysis
    by reading the previously generated relational CSVs.

    :param run_path: The timestamped directory where the relational CSVs were saved.
    """
    logger.info("====== Starting Custom Profile View Generation ======")

    try:
        # Load the primary profiles table
        profiles_df = pd.read_csv(os.path.join(run_path, "profiles.csv"))

        # --- Feature Engineering and Data Transformation ---
        # 1. Calculate description length
        # profiles_df['description_char_count'] = profiles_df['profile_description'].str.len().fillna(0).astype(int)
        profiles_df["description_char_count"] = profiles_df[
            "profile_description"
        ].apply(lambda x: len(x) if isinstance(x, str) else 0)

        # 2. Categorize response time
        def map_response_time(text):
            if not isinstance(text, str):
                return 0
            text = text.lower()
            if "heure" in text:
                return 1
            if "jour" in text:
                return 2
            if "semaine" in text:
                return 3
            return 0

        profiles_df["response_time_category"] = profiles_df["response_time_text"].apply(
            map_response_time
        )

        # --- Load and Pivot One-to-Many Data ---

        # Skills
        try:
            skills_df = pd.read_csv(os.path.join(run_path, "profile_skills.csv"))
            # Count total and certified skills
            skill_counts = skills_df.groupby("profile_id").size().rename("skill_count")
            certified_skill_counts = (
                skills_df[skills_df["is_certified"]]
                .groupby("profile_id")
                .size()
                .rename("certified_skill_count")
            )
            profiles_df = profiles_df.merge(skill_counts, on="profile_id", how="left")
            profiles_df = profiles_df.merge(
                certified_skill_counts, on="profile_id", how="left"
            )

            # Pivot skills into wide format (Compétence 1, 2, ...)
            skills_df["skill_rank"] = skills_df.groupby("profile_id").cumcount() + 1
            skills_pivot = skills_df[skills_df["skill_rank"] <= 10].pivot(
                index="profile_id", columns="skill_rank", values="skill_name"
            )
            skills_pivot.columns = [f"Compétence {i}" for i in skills_pivot.columns]
            profiles_df = profiles_df.merge(skills_pivot, on="profile_id", how="left")
        except FileNotFoundError:
            logger.warning("profile_skills.csv not found, skipping skill processing.")

        # Experiences
        try:
            exp_df = pd.read_csv(os.path.join(run_path, "experiences.csv"))
            exp_df["experience_summary"] = (
                exp_df["title"] + " @ " + exp_df["company_name"]
            )
            exp_df["exp_rank"] = exp_df.groupby("profile_id")["experience_index"].rank(
                method="first"
            )
            exp_pivot = exp_df[exp_df["exp_rank"] <= 5].pivot(
                index="profile_id", columns="exp_rank", values="experience_summary"
            )
            exp_pivot.columns = [f"Expérience {int(i)}" for i in exp_pivot.columns]
            profiles_df = profiles_df.merge(exp_pivot, on="profile_id", how="left")
        except FileNotFoundError:
            logger.warning("experiences.csv not found, skipping experience processing.")

        # Education
        try:
            edu_df = pd.read_csv(os.path.join(run_path, "education.csv"))
            edu_df["edu_rank"] = edu_df.groupby("profile_id")["education_index"].rank(
                method="first"
            )
            edu_pivot = edu_df[edu_df["edu_rank"] <= 3].pivot(
                index="profile_id", columns="edu_rank", values="degree"
            )
            edu_pivot.columns = [f"Formation {int(i)}" for i in edu_pivot.columns]
            profiles_df = profiles_df.merge(edu_pivot, on="profile_id", how="left")
        except FileNotFoundError:
            logger.warning("education.csv not found, skipping education processing.")

        # Languages (Concatenate)
        try:
            lang_df = pd.read_csv(os.path.join(run_path, "profile_languages.csv"))
            lang_agg = (
                lang_df.groupby("profile_id")["language_name"]
                .apply(lambda x: " | ".join(x))
                .rename("languages_list")
            )
            profiles_df = profiles_df.merge(lang_agg, on="profile_id", how="left")
        except FileNotFoundError:
            logger.warning(
                "profile_languages.csv not found, skipping language processing."
            )

        # Badges (for Super Malter status)
        try:
            badge_df = pd.read_csv(os.path.join(run_path, "profile_badges.csv"))
            profiles_df["is_super_malter"] = profiles_df["profile_id"].isin(
                badge_df[badge_df["badge_name"].str.contains("SUPER_MALTER", na=False)][
                    "profile_id"
                ]
            )
        except FileNotFoundError:
            logger.warning(
                "profile_badges.csv not found, cannot determine Super Malter status."
            )

        # --- Final Column Selection and Renaming ---
        column_mapping = {
            "profile_id": "ID",
            "photo_url": "Picture",
            "city": "Localisation",
            "location_type": "Télétravail",
            "availability_status": "Availablility",
            "work_availability": "Full time/Part Time",
            "price_amount": "Tarif",
            "experience_years_text": "Expérience",
            "response_rate_text": "Taux de réponse",
            "response_time_text": "Temps de réponse",
            "stats_missions_count": "Nombre de missions",
            "aggregate_rating_value": "Rates",
            "stats_recommendations_count": "Nombre de recommandations",
            "description_char_count": "Profile description/ Nbr of signs",
            "languages_list": "Langues",
        }

        final_df = profiles_df.rename(columns=column_mapping)

        # Define the exact column order requested
        final_columns = [
            "ID",
            "Gender",
            "Picture",
            "Localisation",
            "Télétravail",
            "Availablility",
            "Full time/Part Time",
            "Tarif",
            "Expérience",
            "Taux de réponse",
            "Temps de réponse",
            "Compétence 1",
            "Compétence 2",
            "Compétence 3",
            "Compétence 4",
            "Compétence 5",
            "Langues",
            "Nombre de missions",
            "Rates",
            "Nombre de recommandations",
            "Profile description/ Nbr of signs",
            "Expérience 1",
            "Expérience 2",
            "Expérience 3",
            "Formation 1",
            "Formation 2",
            # Add our new sociological metrics at the end for extra value
            "skill_count",
            "certified_skill_count",
            "is_super_malter",
            "response_time_category",
        ]

        # Add 'Gender' column as a placeholder, as requested
        final_df["Gender"] = None

        # Reorder the dataframe, adding missing columns as empty
        for col in final_columns:
            if col not in final_df.columns:
                final_df[col] = None

        final_df = final_df[final_columns]

        # Save the final custom CSV
        output_file = os.path.join(run_path, "custom_profile.csv")
        final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        logger.info(
            f"Successfully generated custom profile view with {len(final_df)} rows at {output_file}"
        )

    except Exception as e:
        logger.error(f"Failed to generate custom profile view: {e}", exc_info=True)


# --- Main Execution Function ---
def process_malt_data(
    limit: Optional[int] = None, include_ids: Optional[List[str]] = None
):
    start_time = time.time()
    logger.info("====== Starting Malt Data ETL Process (XPath Parser v3.1.0) ======")
    json_files = find_and_load_json_files(Config.RAW_DATA_ROOT)
    if not json_files:
        logger.error("No raw JSON files found. Aborting.")
        return
    all_profiles = extract_profiles_from_json(json_files)
    total_unique_profiles = len(all_profiles)
    profiles_to_process = {}
    run_type_info = {"type": "full_run", "details": "Processing all found profiles."}

    if include_ids:
        logger.warning(
            f"Processing is limited to {len(include_ids)} specific profile IDs."
        )
        for pid in include_ids:
            if pid in all_profiles:
                profiles_to_process[pid] = all_profiles[pid]
            else:
                logger.warning(f"Requested profile ID '{pid}' not found in raw data.")
        run_type_info = {
            "type": "include_ids_run",
            "ids_requested": len(include_ids),
            "ids_found": len(profiles_to_process),
        }
    elif limit:
        logger.warning(f"Processing is limited to {limit} profiles.")
        profiles_to_process = {
            k: all_profiles[k] for k in list(all_profiles.keys())[:limit]
        }
        run_type_info = {"type": "limit_run", "limit_set": limit}
    else:
        profiles_to_process = all_profiles
    if not profiles_to_process:
        logger.error("No profiles to process after filtering. Aborting.")
        return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_run_path = os.path.join(Config.PROCESSED_DATA_ROOT, run_timestamp)
    os.makedirs(output_run_path, exist_ok=True)
    logger.info(f"Output for this run will be saved in: {output_run_path}")

    processor = MaltDataProcessor(output_run_path)
    logger.info(f"Starting processing for {len(profiles_to_process)} profiles...")
    for profile_id, json_data in tqdm(
        profiles_to_process.items(), desc="Processing Profiles"
    ):
        processor.process_profile(profile_id, json_data)

    processor.save_to_csv()

    # --- NEW: Call the function to create the custom view ---
    create_custom_profile_view(output_run_path)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    run_metadata = {
        "run_timestamp_utc": run_timestamp,
        "duration_seconds": duration,
        "source_json_root": Config.RAW_DATA_ROOT,
        "source_html_root": Config.HTML_ROOT,
        "output_path": output_run_path,
        "total_unique_profiles_found": total_unique_profiles,
        "processing_run_type": run_type_info,
        "profiles_processed_in_run": processor.processed_count,
        "profiles_with_errors": processor.error_count,
        "data_schema_tables": Config.TABLE_NAMES + ["custom_profile"],
    }
    with open(
        os.path.join(output_run_path, "metadata.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(run_metadata, f, indent=2)
    with open(os.path.join(output_run_path, "_SUCCESS"), "w") as f:
        pass

    logger.info(f"====== ETL Process Finished in {duration} seconds ======")
    logger.info(f"Successfully processed: {processor.processed_count} profiles.")
    logger.info(f"Encountered errors on: {processor.error_count} profiles.")


# --- Example Usage Block ---
if __name__ == "__main__":
    process_malt_data(include_ids=["5d70fee8b7d0a40009ce9e00"])
