"""
File: data_quality_check.py
Author: [Your Name]
Date: [Current Date]

Description:
-------------
This module loads raw caregiver profile JSON files from:
    Care-com/raw_data/USA/10002/all_profiles/<id>.json

It performs the following steps:
  1. Loads and flattens the main caregiver fields (e.g., member details, contact info, etc.)
     into a main profiles DataFrame.
  2. Separately extracts and flattens the nested "profiles" field into a Nested_Profiles DataFrame.
     Each non-null sub-profile (e.g., commonCaregiverProfile, houseKeepingCaregiverProfile) is expanded into its own row.
  3. Validates the main profiles DataFrame using pandera.
  4. Performs data quality checks and exports a quality report.
  5. Prints a proposed relational data model (designed in 3NF) as a blueprint for further ETL.

Usage:
------
Run the module from the project root:
    python -m src.etl.data_quality_check
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataQualityCheck")

# Base directory containing raw profile JSON files
BASE_PROFILES_DIR = Path("./raw_data/USA/10002/all_profiles")

def load_json_files(directory: Path) -> list:
    """
    Recursively loads all JSON files (excluding those starting with 'metadata_')
    from the specified directory.

    Args:
        directory (Path): Directory containing JSON files.
    Returns:
        list: List of parsed JSON objects.
    """
    json_objects = []
    for file_path in directory.rglob("*.json"):
        if file_path.name.startswith("metadata_"):
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            json_objects.append(data)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
    return json_objects

def flatten_profile(profile: dict) -> dict:
    """
    Flattens a raw caregiver profile JSON into a single-level dictionary.
    Extracts top-level fields from "getCaregiver" and "member". The entire nested 
    "profiles" object is stored as a JSON string (to be processed separately).

    Args:
        profile (dict): Raw JSON profile.
    Returns:
        dict: Flattened profile dictionary.
    """
    try:
        caregiver = profile["data"]["getCaregiver"]
        member = caregiver.get("member", {})
        flat = {
            "profile_id": member.get("id"),
            "first_name": member.get("firstName"),
            "last_name": member.get("lastName"),
            "gender": member.get("gender"),
            "display_name": member.get("displayName"),
            "email": member.get("email"),
            "primary_service": member.get("primaryService"),
            "hi_res_image": member.get("hiResImageURL"),
            "image_url": member.get("imageURL"),
            "city": member.get("address", {}).get("city"),
            "state": member.get("address", {}).get("state"),
            "zip": member.get("address", {}).get("zip"),
            "languages": json.dumps(member.get("languages", [])),  # stored as JSON string
            "legacy_id": member.get("legacyId"),
            "is_premium": member.get("isPremium", False)
        }
        # Flatten additional caregiver fields
        flat.update({
            "badges": json.dumps(caregiver.get("badges", [])),
            "distance_from_seeker": caregiver.get("distanceFromSeekerInMiles"),
            "background_checks": json.dumps(caregiver.get("backgroundChecks", [])),
            "education_degrees": json.dumps(caregiver.get("educationDegrees", [])),
            "has_care_check": caregiver.get("hasCareCheck"),
            "has_granted_criminal_bgc_access": caregiver.get("hasGrantedCriminalBGCAccess"),
            "has_granted_mvr_bgc_access": caregiver.get("hasGrantedMvrBGCAccess"),
            "has_granted_premier_bgc_access": caregiver.get("hasGrantedPremierBGCAccess"),
            "hired_times": caregiver.get("hiredTimes"),
            "is_favorite": caregiver.get("isFavorite"),
            "is_mvr_eligible": caregiver.get("isMVREligible"),
            "is_vaccinated": caregiver.get("isVaccinated"),
            "place_info": json.dumps(caregiver.get("placeInfo")),
            # Store the nested "profiles" field as a JSON string (to be flattened separately)
            "profiles": json.dumps(caregiver.get("profiles", {})),
            "provider_status": caregiver.get("providerStatus"),
            "recurring_availability": json.dumps(caregiver.get("recurringAvailability", {})),
            "response_rate": caregiver.get("responseRate"),
            "response_time": caregiver.get("responseTime"),
            "sign_up_date": caregiver.get("signUpDate"),
            "years_of_experience": caregiver.get("yearsOfExperience"),
            "non_primary_images": json.dumps(caregiver.get("nonPrimaryImages", []))
        })
        if "continuousBackgroundCheck" in caregiver:
            flat["continuous_background_check"] = json.dumps(caregiver["continuousBackgroundCheck"])
        return flat
    except Exception as e:
        logger.error(f"Error flattening profile: {e}")
        return {}

def create_profiles_dataframe(json_objs: list) -> pd.DataFrame:
    """
    Converts a list of raw profile JSON objects into a pandas DataFrame using flatten_profile().
    
    Args:
        json_objs (list): List of raw profile JSON objects.
    Returns:
        DataFrame: Main profiles DataFrame.
    """
    flattened = [flatten_profile(obj) for obj in json_objs if flatten_profile(obj)]
    df = pd.DataFrame(flattened)
    logger.info(f"Created main profiles DataFrame with {len(df)} records.")
    return df

def extract_nested_profiles(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts and flattens the nested "profiles" field from the main profiles DataFrame.
    For each row, the "profiles" field (a JSON string) is parsed, and each non-null sub-profile 
    (e.g., childCareCaregiverProfile, commonCaregiverProfile, etc.) is flattened into its own row.
    
    Args:
        main_df (DataFrame): Main profiles DataFrame with a "profiles" column.
    Returns:
        DataFrame: Nested_Profiles DataFrame.
    """
    nested_rows = []
    for idx, row in main_df.iterrows():
        profile_id = row.get("profile_id")
        profiles_str = row.get("profiles", "{}")
        try:
            profiles_obj = json.loads(profiles_str)
        except Exception as e:
            logger.error(f"Error parsing profiles JSON for profile_id {profile_id}: {e}")
            continue
        
        # Get serviceIds if available (apply to all sub-profiles)
        service_ids = profiles_obj.get("serviceIds", [])
        
        # Define expected sub-profile keys
        sub_profile_keys = [
            "childCareCaregiverProfile",
            "commonCaregiverProfile",
            "petCareCaregiverProfile",
            "seniorCareCaregiverProfile",
            "tutoringCaregiverProfile",
            "houseKeepingCaregiverProfile"
        ]
        
        for key in sub_profile_keys:
            sub_profile = profiles_obj.get(key)
            if sub_profile is not None:
                flat_sub = {
                    "profile_id": profile_id,
                    "sub_profile_type": key,
                    "service_ids": json.dumps(service_ids)
                }
                # Flatten each field in the sub-profile
                for sub_key, value in sub_profile.items():
                    if isinstance(value, (dict, list)):
                        flat_sub[sub_key] = json.dumps(value)
                    else:
                        flat_sub[sub_key] = value
                nested_rows.append(flat_sub)
    nested_df = pd.DataFrame(nested_rows)
    logger.info(f"Extracted {len(nested_df)} nested profile records.")
    return nested_df

# Define a pandera schema for the main profiles DataFrame
profile_schema = DataFrameSchema({
    "profile_id": Column(str, nullable=False),
    "first_name": Column(str, nullable=True),
    "last_name": Column(str, nullable=True),
    "gender": Column(str, nullable=True),
    "display_name": Column(str, nullable=True),
    "email": Column(str, nullable=True),
    "primary_service": Column(str, nullable=True),
    "hi_res_image": Column(str, nullable=True),
    "image_url": Column(str, nullable=True),
    "city": Column(str, nullable=True),
    "state": Column(str, nullable=True),
    "zip": Column(str, nullable=True),
    "languages": Column(str, nullable=True),
    "legacy_id": Column(str, nullable=True),
    "is_premium": Column(bool, nullable=True),
    "badges": Column(str, nullable=True),
    "distance_from_seeker": Column(object, nullable=True),
    "background_checks": Column(str, nullable=True),
    "education_degrees": Column(str, nullable=True),
    "has_care_check": Column(bool, nullable=True),
    "has_granted_criminal_bgc_access": Column(bool, nullable=True),
    "has_granted_mvr_bgc_access": Column(bool, nullable=True),
    "has_granted_premier_bgc_access": Column(bool, nullable=True),
    "hired_times": Column(object, nullable=True),
    "is_favorite": Column(bool, nullable=True),
    "is_mvr_eligible": Column(bool, nullable=True),
    "is_vaccinated": Column(bool, nullable=True),
    "place_info": Column(str, nullable=True),
    "profiles": Column(str, nullable=True),  # Entire nested profiles as JSON string
    "provider_status": Column(str, nullable=True),
    "recurring_availability": Column(str, nullable=True),
    "response_rate": Column(object, nullable=True),
    "response_time": Column(object, nullable=True),
    "sign_up_date": Column(str, nullable=True),
    "years_of_experience": Column(object, nullable=True),
    "non_primary_images": Column(str, nullable=True),
    "continuous_background_check": Column(str, nullable=True)
}, strict=True)

def safe_json_len(x):
    """
    Safely attempts to parse a JSON string and return its length.
    If parsing fails, logs the error and returns 0.
    """
    try:
        return len(json.loads(x)) if x else 0
    except Exception as e:
        logger.error(f"Error parsing JSON for value {x}: {e}")
        return 0

def export_quality_report(df: pd.DataFrame, output_file: str) -> pd.DataFrame:
    """
    Generates and exports a data quality report for the given DataFrame and saves it as CSV.
    
    Args:
        df (pd.DataFrame): The DataFrame to check.
        output_file (str): Output CSV file path.
    
    Returns:
        pd.DataFrame: Quality report as a DataFrame.
    """
    report = {}
    required_fields = [
        "profile_id", "first_name", "last_name", "display_name", "email",
        "primary_service", "city", "state", "zip", "languages",
        "sign_up_date", "years_of_experience", "gender"
    ]
    for field in required_fields:
        if field in df.columns:
            report[f"{field}_missing"] = int(df[field].isnull().sum())
        else:
            report[f"{field}_missing"] = "Field not found"
    
    # Numeric fields checks for years_of_experience
    if "years_of_experience" in df.columns:
        report["years_of_experience_min"] = df["years_of_experience"].min()
        report["years_of_experience_max"] = df["years_of_experience"].max()
        report["years_of_experience_mean"] = df["years_of_experience"].mean()
    
    # For multi-valued fields stored as JSON strings, use safe_json_len()
    for field in ["languages", "badges", "background_checks", "education_degrees", "recurring_availability", "non_primary_images"]:
        if field in df.columns:
            total = df[field].apply(safe_json_len).sum()
            report[f"total_entries_in_{field}"] = int(total)
    
    quality_df = pd.DataFrame([report])
    quality_df.to_csv(output_file, index=False)
    logger.info(f"Quality report saved to {output_file}")
    return quality_df

def print_data_model():
    """
    Prints the proposed relational data model (3NF) for storing caregiver profiles.
    """
    data_model = """
    Proposed Relational Data Model (3NF):

    1. Profiles (Main Table):
       - profile_id (PK)
       - first_name
       - last_name
       - gender
       - display_name
       - email
       - primary_service
       - hi_res_image
       - image_url
       - city
       - state
       - zip
       - languages
       - legacy_id
       - is_premium
       - badges
       - distance_from_seeker
       - background_checks
       - education_degrees
       - has_care_check
       - has_granted_criminal_bgc_access
       - has_granted_mvr_bgc_access
       - has_granted_premier_bgc_access
       - hired_times
       - is_favorite
       - is_mvr_eligible
       - is_vaccinated
       - place_info
       - provider_status
       - recurring_availability
       - response_rate
       - response_time
       - sign_up_date
       - years_of_experience
       - non_primary_images
       - continuous_background_check

    2. Nested_Profiles (Separate Table):
       - profile_id (FK to Profiles)
       - sub_profile_type (e.g., "childCareCaregiverProfile", "commonCaregiverProfile", etc.)
       - service_ids (JSON string)
       - All flattened sub-profile fields (e.g., approvalStatus, bio, payRange, qualities, etc.)

    3. Reviews (Additional Table):
       - review_id (PK)
       - profile_id (FK to Profiles)
       - care_type
       - create_time
       - delete_time
       - description_display
       - description_original
       - language_code
       - original_source
       - ratings
       - retort
       - reviewer_first_name
       - reviewer_last_initial
       - status
       - update_source
       - update_time
       - verified_by_care

    Additional normalization may involve separate tables for EducationDegrees, Languages, and Services.
    """
    print(data_model)
    
def run_quality_check():
    # Load raw profile JSON files
    profiles_json = load_json_files(BASE_PROFILES_DIR)
    logger.info(f"Loaded {len(profiles_json)} raw profile JSON files from {BASE_PROFILES_DIR}.")

    # Create the main profiles DataFrame using pandas
    profiles_df = create_profiles_dataframe(profiles_json)
    logger.info("Sample of flattened main profiles:")
    logger.info(profiles_df.head())

    # Validate the main profiles DataFrame using pandera
    try:
        validated_df = profile_schema.validate(profiles_df, lazy=True)
        logger.info("Main profiles DataFrame passed schema validation.")
    except pa.errors.SchemaErrors as err:
        logger.error("Schema validation errors in main profiles:")
        logger.error(err.failure_cases)
        validated_df = profiles_df  # Optionally, proceed with unvalidated data

    # Export quality report for main profiles
    quality_report_file = os.path.join(BASE_PROFILES_DIR.parent, "data_quality_report_profiles.csv")
    quality_df = export_quality_report(validated_df, quality_report_file)
    logger.info("Main Profiles Quality Report:")
    logger.info(quality_df)

    # Extract nested "profiles" into a separate DataFrame (flatten sub-profiles)
    nested_profiles_df = extract_nested_profiles(validated_df)
    logger.info("Sample of extracted nested profiles:")
    logger.info(nested_profiles_df.head())

    # Export nested profiles to CSV for further ETL
    nested_output_file = os.path.join(BASE_PROFILES_DIR.parent, "nested_profiles.csv")
    nested_profiles_df.to_csv(nested_output_file, index=False)
    logger.info(f"Nested profiles exported to {nested_output_file}")

    # Print the proposed relational data model
    print_data_model()


if __name__ == "__main__":
    # Load raw profile JSON files
    profiles_json = load_json_files(BASE_PROFILES_DIR)
    logger.info(f"Loaded {len(profiles_json)} raw profile JSON files from {BASE_PROFILES_DIR}.")

    # Create the main profiles DataFrame using pandas
    profiles_df = create_profiles_dataframe(profiles_json)
    logger.info("Sample of flattened main profiles:")
    logger.info(profiles_df.head())

    # Validate the main profiles DataFrame using pandera
    try:
        validated_df = profile_schema.validate(profiles_df, lazy=True)
        logger.info("Main profiles DataFrame passed schema validation.")
    except pa.errors.SchemaErrors as err:
        logger.error("Schema validation errors in main profiles:")
        logger.error(err.failure_cases)
        validated_df = profiles_df  # Optionally, proceed with unvalidated data

    # Export quality report for main profiles
    quality_report_file = os.path.join(BASE_PROFILES_DIR.parent, "data_quality_report_profiles.csv")
    quality_df = export_quality_report(validated_df, quality_report_file)
    logger.info("Main Profiles Quality Report:")
    logger.info(quality_df)

    # Extract nested "profiles" into a separate DataFrame (flatten sub-profiles)
    nested_profiles_df = extract_nested_profiles(validated_df)
    logger.info("Sample of extracted nested profiles:")
    logger.info(nested_profiles_df.head())

    # Export nested profiles to CSV for further ETL
    nested_output_file = os.path.join(BASE_PROFILES_DIR.parent, "nested_profiles.csv")
    nested_profiles_df.to_csv(nested_output_file, index=False)
    logger.info(f"Nested profiles exported to {nested_output_file}")

    # Print the proposed relational data model
    print_data_model()


