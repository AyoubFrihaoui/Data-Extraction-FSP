"""
File: src/etl/data_quality_check.py
Author: @AyoubFrihaoui
Date: 03-25-2025

Description:
-------------
This script aggregates caregiver profile JSON files from multiple cities in France. The raw data is
organized under:
    data/raw_data/France/<postal_code>/<profile_folder>/
For example:
    data/raw_data/France/75001/Profiles_of_childCare
    data/raw_data/France/75001/Profiles_of_childCare__babysitter
    data/raw_data/France/75001/Profiles_of_seniorCare

For each profile file (excluding files whose names start with "metada_"), the script:
  - Adds a "postal_code" column (extracted from the parent folder name).
  - Enriches the data with the corresponding city name using pgeocode.
  - Adds a "source_folder" column (the profile category such as "Profiles_of_childCare").

After aggregation, the script performs data quality checks by:
  - Generating a missing value report.
  - Validating the aggregated DataFrame against a defined schema using pandera.
  
The output includes two CSV files stored in the "data/processed" folder:
  - "aggregated_profiles.csv" containing the full data.
  - "data_quality_report.csv" containing the missing value report.

This preprocessed output is designed to be used in subsequent ETL steps and SQL-based modeling
(e.g., designing a relational database in 3NF for use in Power BI).
"""

import os
import json
from pathlib import Path
import pandas as pd
import numpy as np
import logging
import pgeocode
import pandera as pa
from pandera import Column, DataFrameSchema, Check

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataQualityCheck")

def get_city_from_postal_code(postal_code: str, cache: dict) -> str:
    """
    Retrieves the city name for a given French postal code using pgeocode.
    
    Args:
        postal_code (str): The French postal code (e.g., "75001").
        cache (dict): A dictionary for caching postal code to city mappings.
        
    Returns:
        str: The city name if found; otherwise, "Unknown".
    """
    if postal_code in cache:
        return cache[postal_code]
    try:
        nomi = pgeocode.Nominatim('fr')
        res = nomi.query_postal_code(postal_code)
        city = res.place_name if pd.notnull(res.place_name) else "Unknown"
    except Exception as e:
        logger.error(f"Error retrieving city for postal code {postal_code}: {e}")
        city = "Unknown"
    cache[postal_code] = city
    return city

def load_profiles_from_directory(directory: str, postal_code: str, city: str) -> pd.DataFrame:
    """
    Loads all profile JSON files from the specified directory into a DataFrame.
    It skips files whose names start with 'metada_' (metadata files) and adds 
    "postal_code", "city", and "source_folder" columns.
    
    Args:
        directory (str): Path to the directory containing profile JSON files.
        postal_code (str): The postal code for the profiles.
        city (str): The city corresponding to the postal code.
    
    Returns:
        pd.DataFrame: DataFrame containing profile data.
    """
    profiles = []
    directory_path = Path(directory)
    source_folder = directory_path.name  # e.g., "Profiles_of_childCare"
    for file_path in directory_path.glob("*.json"):
        if file_path.name.startswith("metada_"):
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                profile = json.load(f)
                profile["postal_code"] = postal_code
                profile["city"] = city
                profile["source_folder"] = source_folder
                profiles.append(profile)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
    if profiles:
        df = pd.DataFrame(profiles)
        logger.info(f"Loaded {len(df)} profiles from {directory} for postal code {postal_code} ({city}).")
    else:
        df = pd.DataFrame()
        logger.warning(f"No profiles found in {directory} for postal code {postal_code}.")
    return df

def aggregate_profiles_with_city_info(base_dir: str) -> pd.DataFrame:
    """
    Aggregates profiles from multiple postal code folders under the base directory.
    Each sub-folder in base_dir is expected to be a postal code folder.
    For each postal code, profiles are loaded from sub-folders starting with "Profiles_of_",
    and the DataFrame is enriched with "postal_code", "city", and "source_folder" columns.
    
    Args:
        base_dir (str): Base directory for raw data, e.g., "data/raw_data/France".
    
    Returns:
        pd.DataFrame: Aggregated DataFrame containing all profiles (with duplicates).
    """
    profile_dfs = []
    city_cache = {}
    base_path = Path(base_dir)
    for postal_dir in base_path.iterdir():
        if postal_dir.is_dir():
            postal_code = postal_dir.name
            city = get_city_from_postal_code(postal_code, city_cache)
            for subfolder in postal_dir.iterdir():
                if subfolder.is_dir() and subfolder.name.startswith("Profiles_of_"):
                    df = load_profiles_from_directory(str(subfolder), postal_code, city)
                    if not df.empty:
                        profile_dfs.append(df)
    if profile_dfs:
        combined_df = pd.concat(profile_dfs, ignore_index=True)
        logger.info(f"Aggregated {len(combined_df)} profiles from all postal codes.")
    else:
        combined_df = pd.DataFrame()
        logger.warning("No profiles aggregated from provided directories.")
    return combined_df

def remove_duplicate_caregivers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate caregivers (rows with the same 'id') by merging their 'source_folder' 
    into a list. For all other columns, the first non-null row is kept.
    
    Args:
        df (pd.DataFrame): The aggregated profiles DataFrame (potentially with duplicates).
    
    Returns:
        pd.DataFrame: A DataFrame where each caregiver (id) is unique, and 'source_folder'
                      is a list of all folders in which the caregiver was found.
    """
    if df.empty:
        return df

    # Ensure 'id' is numeric for grouping; any non-numeric will become NaN
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    # Drop rows where 'id' is missing
    df = df.dropna(subset=["id"])
    # Convert 'id' to int
    df.loc["id"] = df["id"].astype(int)

    # Build a dynamic aggregator: 'first' for all columns except 'source_folder',
    # which is merged into a list (set) of unique folder names.
    aggregator = {}
    for col in df.columns:
        if col == "source_folder":
            aggregator[col] = lambda x: list(set(x))
        else:
            aggregator[col] = "first"

    grouped_df = df.groupby("id", as_index=False).agg(aggregator)
    logger.info(f"Removed duplicates: final row count is {len(grouped_df)} (from {len(df)} original rows).")
    return grouped_df

def generate_missing_value_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a report summarizing missing values in the DataFrame, including total and percentage missing
    for each column and categorizes the impact as High, Medium, or Low.
    
    Args:
        df (pd.DataFrame): DataFrame containing profile data.
    
    Returns:
        pd.DataFrame: A summary DataFrame with missing value statistics.
    """
    missing_stats = pd.DataFrame({
        'total_missing': df.isnull().sum(),
        'percent_missing': (df.isnull().sum() / len(df) * 100).round(2)
    }).sort_values('percent_missing', ascending=False)
    missing_stats['impact'] = missing_stats['percent_missing'].apply(
        lambda x: 'High' if x > 15 else ('Medium' if x > 5 else 'Low')
    )
    return missing_stats

def run_pandera_checks(df: pd.DataFrame) -> dict:
    """
    Validates the aggregated DataFrame using pandera against a defined schema.
    The schema checks required columns, their data types, and plausible numeric ranges.
    
    Returns:
        dict: The validation report (raises an exception if validation fails).
    """
    
    schema = DataFrameSchema({
        "id": Column(int, nullable=False, checks=Check(lambda s: s > 0, element_wise=True)),
        "verticalId": Column(str, nullable=False),
        "displayName": Column(str, nullable=True),
        "hourlyRate": Column(float, nullable=True, checks=Check(lambda s: (s >= 0) & (s <= 500))),
        "yearsOfExperience": Column(float, nullable=True, checks=Check(lambda s: (s >= 0) & (s <= 100))),
        "location": Column(str, nullable=True),
        "memberSince": Column(str, nullable=True),
        "age": Column(int, nullable=True, checks=Check(lambda s: (s >= 0) & (s <= 120))),
        "postal_code": Column(str, nullable=False),
        "city": Column(str, nullable=True),
        "source_folder": Column(object, nullable=False)
    }, coerce=True)
    
    try:
        validated_df = schema.validate(df, lazy=True)
        logger.info("Pandera validation succeeded.")
        return {"success": True, "validation_errors": None}
    except pa.errors.SchemaErrors as err:
        logger.error("Pandera validation failed.")
        logger.error(err)
        return {"success": False, "validation_errors": err.failure_cases.to_dict(orient="records")}

if __name__ == "__main__":
    base_dir = os.path.join("data", "raw_data", "France")
    profiles_df = aggregate_profiles_with_city_info(base_dir)
    if profiles_df.empty:
        logger.error("No profile data aggregated for quality checks.")
    else:
        missing_report = generate_missing_value_report(profiles_df)
        logger.info("Missing Value Report:")
        logger.info(missing_report)
        
        # Run pandera-based data quality checks
        pandera_report = run_pandera_checks(profiles_df)
        logger.info("Pandera Data Quality Check Report:")
        logger.info(pandera_report)
        
        # Save the aggregated profiles and data quality report
        processed_dir = os.path.join("data", "processed")
        os.makedirs(processed_dir, exist_ok=True)
        profiles_output_file = os.path.join(processed_dir, "aggregated_profiles.csv")
        dq_report_file = os.path.join(processed_dir, "data_quality_report.csv")
        
        profiles_df.to_csv(profiles_output_file, index=False)
        missing_report.to_csv(dq_report_file, index=False)
        
        logger.info(f"Aggregated profiles saved to {profiles_output_file}")
        logger.info(f"Data quality report saved to {dq_report_file}")

def run_check():
    try:
        base_dir = os.path.join("..", "data", "raw_data", "France")
        profiles_df = aggregate_profiles_with_city_info(base_dir)
        if profiles_df.empty:
            logger.error("No profile data aggregated for quality checks.")
        else:
            # 1) Remove duplicates by merging 'source_folder' into a list
            profiles_df = remove_duplicate_caregivers(profiles_df)
            
            # 2) Generate a missing value report
            missing_report = generate_missing_value_report(profiles_df)
            logger.info("Missing Value Report:")
            logger.info(missing_report)
            # Drop rows where required fields are missing
            required_fields = ["id", "verticalId", "postal_code"]
            cleaned_df = profiles_df.dropna(subset=required_fields)
            logger.info(f"Cleaned DataFrame after dropping rows with missing required fields: {len(cleaned_df)} rows.")

            # 3) Run pandera-based data quality checks
            pandera_report = run_pandera_checks(cleaned_df)
            logger.info("Pandera Data Quality Check Report:")
            logger.info(pandera_report)

            # 4) Save the aggregated profiles and data quality report
            processed_dir = os.path.join("..", "data", "processed")
            os.makedirs(processed_dir, exist_ok=True)
            profiles_output_file = os.path.join(processed_dir, "aggregated_profiles.csv")
            dq_report_file = os.path.join(processed_dir, "data_quality_report.csv")

            profiles_df.to_csv(profiles_output_file, index=False, encoding="utf-8")
            missing_report.to_csv(dq_report_file, index=False)

            logger.info(f"Aggregated profiles saved to {profiles_output_file}")
            logger.info(f"Data quality report saved to {dq_report_file}")
    except Exception as e:
        logger.error(f"An error occurred during data quality checks: {e}")