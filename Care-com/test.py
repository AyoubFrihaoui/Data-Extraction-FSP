# data_quality_check_hybrid_flattening.py

import os
import json
import logging
from pathlib import Path
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
import traceback # For detailed error logging

# --- Configuration (ADJUST THESE PATHS AND VALUES) ---
BASE_RAW_DIR = Path("raw_data")
BASE_PREPROCESSED_DIR = Path("preprocessed_data")
EXAMPLE_ZIP = "10002" # Or "07008" etc.
COUNTRY = "USA"

# --- Dynamic Path Construction ---
COUNTRY_RAW_DIR = BASE_RAW_DIR / COUNTRY
ZIP_RAW_DIR = COUNTRY_RAW_DIR / EXAMPLE_ZIP
PROFILES_INPUT_DIR = ZIP_RAW_DIR / "all_profiles"
REVIEWS_INPUT_DIR = ZIP_RAW_DIR / "reviews"

COUNTRY_PREPROCESSED_DIR = BASE_PREPROCESSED_DIR / COUNTRY
ZIP_PREPROCESSED_DIR = COUNTRY_PREPROCESSED_DIR / EXAMPLE_ZIP
PROFILES_OUTPUT_DIR = ZIP_PREPROCESSED_DIR
REVIEWS_OUTPUT_DIR = ZIP_PREPROCESSED_DIR # Saving reviews in the same zip folder

# --- Logging Setup ---
LOG_FILE = BASE_PREPROCESSED_DIR / f"data_quality_{COUNTRY}_{EXAMPLE_ZIP}.log"
# Ensure log directory exists *before* setting up logging handlers
if not LOG_FILE.parent.exists():
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"Created log directory: {LOG_FILE.parent}")
    except Exception as e:
        print(f"FATAL: Could not create log directory {LOG_FILE.parent}: {e}")
        exit(1) # Exit if logging can't be set up
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def ensure_dir_exists(path: Path):
    """Creates a directory if it doesn't exist."""
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {e}")
            raise

def load_json_files(directory: Path) -> list:
    """Recursively loads JSON files (excluding 'metadata_') from a directory."""
    json_objects = []
    if not directory.is_dir():
        logger.warning(f"Input directory not found or is not a directory: {directory}")
        return json_objects
    logger.info(f"Scanning for JSON files in: {directory}")
    file_count = 0
    for file_path in directory.rglob("*.json"):
        file_count += 1
        if file_path.name.startswith("metadata_"): continue
        try:
            with open(file_path, "r", encoding="utf-8") as f: data = json.load(f)
            json_objects.append({"filename": str(file_path.relative_to(BASE_RAW_DIR)), "data": data})
        except json.JSONDecodeError as e: logger.error(f"JSON Decode Error in file {file_path}: {e}")
        except Exception as e: logger.error(f"Error reading file {file_path}: {e}")
    logger.info(f"Scanned {file_count} files. Found {len(json_objects)} valid JSON files in {directory}.")
    return json_objects

def safe_json_dump(data, default="null"):
    """Safely dumps data to JSON string, handling potential type errors."""
    try:
        return json.dumps(data)
    except TypeError as e:
        logger.warning(f"Could not serialize data to JSON: {e}. Data type: {type(data)}. Returning '{default}'.")
        return default # Return valid JSON null or empty object/list string

def flatten_main_profile(profile_data: dict, filename: str) -> dict:
    """
    Flattens the main caregiver profile fields explicitly.
    Stores complex nested objects (profiles, backgroundChecks, etc.) as JSON strings.
    """
    flat_profile = {"source_filename": filename, "_flattening_error": False}
    try:
        # Basic structure check
        if not isinstance(profile_data.get("data", {}).get("getCaregiver"), dict):
             logger.error(f"Unexpected JSON structure in {filename}. Missing 'data.getCaregiver'.")
             flat_profile["_flattening_error"] = True
             return flat_profile

        caregiver = profile_data["data"]["getCaregiver"]
        member = caregiver.get("member", {})
        address = member.get("address", {}) if isinstance(member, dict) else {}

        # --- Explicitly flatten member fields ---
        flat_profile.update({
            "profile_id": member.get("id"),
            "member_firstName": member.get("firstName"),
            "member_lastName": member.get("lastName"),
            "member_gender": member.get("gender"),
            "member_displayName": member.get("displayName"),
            "member_email": member.get("email"),
            "member_primaryService": member.get("primaryService"),
            "member_hiResImageURL": member.get("hiResImageURL"),
            "member_imageURL": member.get("imageURL"),
            "member_address_city": address.get("city"),
            "member_address_state": address.get("state"),
            "member_address_zip": address.get("zip"),
            "member_languages_json": safe_json_dump(member.get("languages", [])),
            "member_legacyId": member.get("legacyId"),
            "member_isPremium": member.get("isPremium", None) # Keep None for now
        })

        # --- Explicitly flatten top-level caregiver fields ---
        flat_profile.update({
            "distanceFromSeekerInMiles": caregiver.get("distanceFromSeekerInMiles"),
            "hasCareCheck": caregiver.get("hasCareCheck"),
            "hasGrantedCriminalBGCAccess": caregiver.get("hasGrantedCriminalBGCAccess"),
            "hasGrantedMvrBGCAccess": caregiver.get("hasGrantedMvrBGCAccess"),
            "hasGrantedPremierBGCAccess": caregiver.get("hasGrantedPremierBGCAccess"),
            "hiredTimes": caregiver.get("hiredTimes"),
            "isFavorite": caregiver.get("isFavorite"),
            "isMVREligible": caregiver.get("isMVREligible"),
            "isVaccinated": caregiver.get("isVaccinated"),
            "providerStatus": caregiver.get("providerStatus"),
            "responseRate": caregiver.get("responseRate"),
            "responseTime": caregiver.get("responseTime"),
            "signUpDate": caregiver.get("signUpDate"),
            "yearsOfExperience": caregiver.get("yearsOfExperience"),
            # --- Store complex fields as JSON ---
            "badges_json": safe_json_dump(caregiver.get("badges", [])),
            "backgroundChecks_json": safe_json_dump(caregiver.get("backgroundChecks", [])),
            "educationDegrees_json": safe_json_dump(caregiver.get("educationDegrees", [])),
            "hiredByCounts_json": safe_json_dump(caregiver.get("hiredByCounts", {})),
            "placeInfo_json": safe_json_dump(caregiver.get("placeInfo", {})),
            "profiles_json": safe_json_dump(caregiver.get("profiles", {})), # Key for nested processing
            "recurringAvailability_json": safe_json_dump(caregiver.get("recurringAvailability", {})),
            "nonPrimaryImages_json": safe_json_dump(caregiver.get("nonPrimaryImages", [])),
            "continuousBackgroundCheck_json": safe_json_dump(caregiver.get("continuousBackgroundCheck", {}))
        })

        # --- Type Conversion (using pd.NA for missing/failed) ---
        bool_fields = [k for k in flat_profile if k.startswith('is') or k.startswith('has')]
        for field in bool_fields:
            val = flat_profile.get(field)
            if val is None: flat_profile[field] = pd.NA
            else: 
                try: flat_profile[field] = bool(val)
                except: flat_profile[field] = pd.NA

        numeric_fields = ["hiredTimes", "responseRate", "responseTime", "yearsOfExperience", "distanceFromSeekerInMiles"]
        for field in numeric_fields:
             val = flat_profile.get(field)
             if val is None: flat_profile[field] = pd.NA
             else:
                try: flat_profile[field] = pd.to_numeric(val) if str(val).strip() != "" else pd.NA
                except (ValueError, TypeError): flat_profile[field] = pd.NA

        if flat_profile["signUpDate"]:
             try: flat_profile["signUpDate"] = pd.to_datetime(flat_profile["signUpDate"], errors='coerce')
             except Exception: flat_profile["signUpDate"] = pd.NaT
        else: flat_profile["signUpDate"] = pd.NaT

        # Check profile_id after potential member issues
        if pd.isna(flat_profile["profile_id"]):
             logger.warning(f"Missing profile_id for record from {filename}.")

        return flat_profile
    except Exception as e:
        logger.error(f"Critical error flattening main profile in file {filename}: {e}\n{traceback.format_exc()}")
        flat_profile["_flattening_error"] = True
        return flat_profile

def extract_and_flatten_nested_profiles(main_profile_row: pd.Series) -> list:
    """
    Extracts sub-profiles. Flattens known simple fields explicitly.
    Dynamically flattens known nested dicts (qualities, services, etc.).
    Stores known lists as JSON.
    """
    nested_rows = []
    profile_id = main_profile_row.get("profile_id")
    filename = main_profile_row.get("source_filename", "unknown")
    profiles_str = main_profile_row.get("profiles_json", "{}") # Key from flatten_main_profile

    if pd.isna(profile_id) or profile_id is None:
        # logger.warning(f"Skipping nested: missing profile_id in row from {filename}") # Too verbose
        return nested_rows

    try:
        profiles_obj = json.loads(profiles_str) if isinstance(profiles_str, str) and profiles_str else {}
        if not isinstance(profiles_obj, dict):
             logger.warning(f"Parsed 'profiles_json' is not a dict for profile_id {profile_id} from {filename}. Skipping.")
             return nested_rows
    except Exception as e:
        logger.error(f"Error parsing nested profiles JSON for profile_id {profile_id} from {filename}: {e}. JSON: '{str(profiles_str)[:100]}...'")
        return nested_rows

    service_ids = profiles_obj.get("serviceIds", []) # List
    # Known sub-profile keys
    sub_profile_keys = [
        "childCareCaregiverProfile", "commonCaregiverProfile", "petCareCaregiverProfile",
        "seniorCareCaregiverProfile", "tutoringCaregiverProfile", "houseKeepingCaregiverProfile"
    ]
    # Known nested dictionaries within sub-profiles that need dynamic inner flattening
    dynamic_inner_dicts = ['bio', 'payRange', 'recurringRate', 'qualities', 'supportedServices', 'merchandizedJobInterests']
    # Known lists within sub-profiles to store as JSON
    json_lists = ['ageGroups', 'otherQualities', 'rates', 'schedule', 'educationDetailsText']


    for sub_profile_type in sub_profile_keys:
        sub_profile = profiles_obj.get(sub_profile_type)
        if isinstance(sub_profile, dict): # Process only if it's a dictionary
            try:
                # Base info
                flat_sub = {
                    "profile_id": profile_id,
                    "sub_profile_type": sub_profile_type,
                    "service_ids_json": safe_json_dump(service_ids)
                }
                prefix = sub_profile_type + "_" # Prefix for column names

                # Iterate through fields in the sub-profile
                for field, value in sub_profile.items():
                    col_name_base = prefix + field
                    if field == "__typename": continue # Skip metadata

                    # 1. Handle known dictionaries needing dynamic inner flattening
                    if field in dynamic_inner_dicts and isinstance(value, dict):
                        for inner_key, inner_val in value.items():
                            if inner_key == "__typename": continue
                            # Create column like: houseKeepingCaregiverProfile_qualities_doesNotSmoke
                            flat_sub[f"{col_name_base}_{inner_key}"] = inner_val
                    # 2. Handle known lists to store as JSON
                    elif field in json_lists and isinstance(value, list):
                        flat_sub[f"{col_name_base}_json"] = safe_json_dump(value)
                    # 3. Handle other nested lists/dicts (store as JSON as fallback)
                    elif isinstance(value, list):
                         logger.debug(f"Storing unhandled list field '{field}' as JSON for {sub_profile_type}, profile {profile_id}")
                         flat_sub[f"{col_name_base}_json"] = safe_json_dump(value)
                    elif isinstance(value, dict):
                         logger.debug(f"Storing unhandled dict field '{field}' as JSON for {sub_profile_type}, profile {profile_id}")
                         flat_sub[f"{col_name_base}_json"] = safe_json_dump(value)
                    # 4. Handle simple, explicitly known fields
                    else:
                         flat_sub[col_name_base] = value

                # --- Optional: Type Conversion after flattening ---
                # Convert numeric/bool fields based on their *flattened* names
                for key, val in flat_sub.items():
                     # Skip already processed or non-primitive types
                    if pd.isna(val) or isinstance(val, (bool, int, float, list, dict)): continue

                    # Attempt numeric
                    if any(suffix in key for suffix in ['Rate', 'Count', 'Experience', 'amount', 'year', 'age', 'ratio', 'distance', 'hourlyRateFrom', 'hourlyRateTo']):
                         try: flat_sub[key] = pd.to_numeric(val) if str(val).strip() != "" else pd.NA
                         except (ValueError, TypeError): pass # Keep original if failed

                    # Attempt boolean (for flags within qualities, services etc.)
                    # Check if it looks like a flag from known nested dicts
                    elif any(f"_{flag_dict}_" in key for flag_dict in dynamic_inner_dicts):
                        if isinstance(val, str) and val.lower() in ['true', 'false']:
                            flat_sub[key] = val.lower() == 'true'
                        elif str(val) in ['1', '0']:
                            flat_sub[key] = str(val) == '1'
                        # If it's already bool, it's fine. Otherwise leave as is.

                nested_rows.append(flat_sub)
            except Exception as e:
                 logger.error(f"Error hybrid flattening sub-profile '{sub_profile_type}' for profile_id {profile_id} from {filename}: {e}\n{traceback.format_exc()}")
        elif sub_profile is not None:
             logger.debug(f"Skipping non-dict sub-profile '{sub_profile_type}' (type: {type(sub_profile)}) for profile_id {profile_id}.")

    return nested_rows


def flatten_review(review_data: dict, filename: str, reviewee_id_override: str = None) -> dict:
    """Flattens a single review object explicitly, storing complex parts as JSON."""
    flat_review = {"source_filename": filename, "_flattening_error": False}
    try:
        if not isinstance(review_data, dict):
             logger.warning(f"Review data is not a dict in {filename}. Skipping.")
             flat_review["_flattening_error"] = True; return flat_review

        review_id = review_data.get("id")
        flat_review["review_id"] = review_id

        description = review_data.get("description", {})
        reviewee = review_data.get("reviewee", {})
        reviewer = review_data.get("reviewer", {})
        public_member_info = reviewer.get("publicMemberInfo", {}) if isinstance(reviewer, dict) else {}

        profile_id = reviewee.get("id") if isinstance(reviewee, dict) else None
        if not profile_id and reviewee_id_override: profile_id = reviewee_id_override
        flat_review["profile_id"] = profile_id

        flat_review.update({
            "careType": review_data.get("careType"),
            "createTime": review_data.get("createTime"),
            "deleteTime": review_data.get("deleteTime"),
            "description_displayText": description.get("displayText") if isinstance(description, dict) else None,
            "description_originalText": description.get("originalText") if isinstance(description, dict) else None,
            "languageCode": review_data.get("languageCode"),
            "originalSource": review_data.get("originalSource"),
            "status": review_data.get("status"),
            "updateSource": review_data.get("updateSource"),
            "updateTime": review_data.get("updateTime"),
            "verifiedByCare": review_data.get("verifiedByCare", None),
            # Reviewee info
            "reviewee_providerType": reviewee.get("providerType") if isinstance(reviewee, dict) else None,
            "reviewee_type": reviewee.get("type") if isinstance(reviewee, dict) else None,
            # Reviewer info
            "reviewer_imageURL": reviewer.get("imageURL") if isinstance(reviewer, dict) else None,
            "reviewer_firstName": public_member_info.get("firstName") if isinstance(public_member_info, dict) else None,
            "reviewer_lastInitial": public_member_info.get("lastInitial") if isinstance(public_member_info, dict) else None,
            "reviewer_source": reviewer.get("source") if isinstance(reviewer, dict) else None,
            "reviewer_type": reviewer.get("type") if isinstance(reviewer, dict) else None,
            # JSON fields
            "ratings_json": safe_json_dump(review_data.get("ratings", [])),
            "retort_json": safe_json_dump(review_data.get("retort", {})),
            "attributes_json": safe_json_dump(review_data.get("attributes", []))
        })

        # Type Conversion
        for ts_field in ["createTime", "deleteTime", "updateTime"]:
            val = flat_review.get(ts_field)
            if val:
                try: flat_review[ts_field] = pd.to_datetime(val, errors='coerce')
                except Exception: flat_review[ts_field] = pd.NaT
            else: flat_review[ts_field] = pd.NaT

        val_bool = flat_review.get("verifiedByCare")
        if val_bool is None: flat_review["verifiedByCare"] = pd.NA
        else:
            try: flat_review["verifiedByCare"] = bool(val_bool)
            except: flat_review["verifiedByCare"] = pd.NA

        return flat_review
    except Exception as e:
        logger.error(f"Critical error flattening review in file {filename} (ID: {review_id}): {e}\n{traceback.format_exc()}")
        flat_review["_flattening_error"] = True
        return flat_review

def create_dataframe(json_objs: list, flatten_func: callable, desc: str) -> pd.DataFrame:
    """Creates DataFrame by applying flattening function, skipping errors."""
    flattened_data = []
    error_count = 0
    for item in json_objs:
        if isinstance(item.get('data'), dict):
            flat_item = flatten_func(item['data'], item['filename'])
            if isinstance(flat_item, dict) and not flat_item.get("_flattening_error"):
                flattened_data.append(flat_item)
            else: error_count += 1 # Count records that failed to flatten
        else: logger.warning(f"Skipping record from {item['filename']}: invalid 'data' structure.")
    if error_count > 0: logger.error(f"{error_count} records failed to flatten during {desc} creation.")
    if not flattened_data: logger.warning(f"No valid data to create DataFrame for {desc}."); return pd.DataFrame()
    df = pd.DataFrame(flattened_data)
    df = df.convert_dtypes()
    logger.info(f"Created DataFrame for {desc} with {len(df)} records and {len(df.columns)} columns.")
    return df

def create_reviews_dataframe(json_objs: list) -> pd.DataFrame:
    """Creates DataFrame for reviews, handling nested list structure."""
    all_reviews = []
    error_count = 0
    for item in json_objs:
        filename = item['filename']
        try:
            data_level = item.get('data', {})
            payload_level = data_level.get('data', {})
            review_payload = payload_level.get('reviewsByReviewee', {})
            reviews_list = review_payload.get('reviews', [])
            if not isinstance(reviews_list, list): logger.warning(f"Reviews not a list in {filename}."); continue
            reviewee_id_from_payload = None # Placeholder
            for review_obj in reviews_list:
                if isinstance(review_obj, dict):
                    flat_review = flatten_review(review_obj, filename, reviewee_id_from_payload)
                    if isinstance(flat_review, dict) and not flat_review.get("_flattening_error"): all_reviews.append(flat_review)
                    else: error_count += 1
                else: logger.warning(f"Review item not a dict in {filename}.")
        except Exception as e: logger.error(f"Error processing review payload {filename}: {e}\n{traceback.format_exc()}"); error_count += 1
    if error_count > 0: logger.error(f"{error_count} reviews failed to flatten.")
    if not all_reviews: logger.warning("No valid review data found."); return pd.DataFrame()
    df = pd.DataFrame(all_reviews)
    df = df.convert_dtypes()
    logger.info(f"Created DataFrame for reviews with {len(df)} records and {len(df.columns)} columns.")
    return df

# --- Pandera Schemas (Explicit Core Fields, Non-Strict for Dynamic Nested) ---
main_profile_schema = DataFrameSchema({
    "profile_id": Column(str, nullable=False, unique=True), # Expect this to be non-null after fixing potential issues
    "source_filename": Column(str, nullable=False),
    "member_firstName": Column(str, nullable=True, coerce=True),
    "member_gender": Column(str, nullable=True, coerce=True),
    "member_isPremium": Column(bool, nullable=True, coerce=True),
    "yearsOfExperience": Column(float, nullable=True, coerce=True), # Using float due to pd.NA
    "signUpDate": Column("datetime64[ns]", nullable=True, coerce=True),
    "profiles_json": Column(str, nullable=True), # Needed for nested extraction
}, strict=False, coerce=True) # strict=False allows dynamic member_ fields and caregiver_ fields

nested_profile_schema = DataFrameSchema({
    "profile_id": Column(str, nullable=False),
    "sub_profile_type": Column(str, nullable=False),
    # Known simple fields (examples)
    "commonCaregiverProfile_id": Column(str, nullable=True, coerce=True, required=False), # Make optional as it depends on type
    "commonCaregiverProfile_repeatClientsCount": Column(float, nullable=True, coerce=True, required=False), # Float for NA
    "seniorCareCaregiverProfile_yearsOfExperience": Column(float, nullable=True, coerce=True, required=False),
    "houseKeepingCaregiverProfile_yearsOfExperience": Column(float, nullable=True, coerce=True, required=False),
    # Add more explicit simple fields if needed, but keep strict=False
}, strict=False, coerce=True) # strict=False essential for dynamic *_qualities_*, *_services_* etc.

review_schema = DataFrameSchema({
    "review_id": Column(str, nullable=False, unique=True),
    "profile_id": Column(str, nullable=True), # Allow null if link failed initially
    "createTime": Column("datetime64[ns]", nullable=True, coerce=True),
    "verifiedByCare": Column(bool, nullable=True, coerce=True),
    "ratings_json": Column(str, nullable=True),
    "attributes_json": Column(str, nullable=True),
}, strict=False, coerce=True)


# --- Data Quality & Reporting ---
# run_quality_checks_and_report and safe_json_load functions remain the same as the previous good version
# (Re-include them here for completeness)
def safe_json_load(json_string):
    """Safely load JSON string, return empty list/dict on failure."""
    if pd.isna(json_string) or not isinstance(json_string, str): return []
    try:
        loaded = json.loads(json_string)
        return loaded if isinstance(loaded, (list, dict)) else []
    except Exception: return []

def run_quality_checks_and_report(df: pd.DataFrame, schema: DataFrameSchema, df_name: str, output_dir: Path) -> pd.DataFrame:
    """Performs validation, calculates quality metrics, saves report/validated data."""
    logger.info(f"--- Running Quality Checks for {df_name} ({len(df)} rows) ---")
    if df.empty: logger.warning(f"DataFrame '{df_name}' is empty."); return df

    logger.info(f"Data types PRE-validation for {df_name}:\n{df.dtypes.value_counts()}")

    validated_df, validation_errors = df, None
    try:
        validated_df = schema.validate(df.copy(), lazy=True)
        logger.info(f"Schema validation PASSED for {df_name}.")
    except pa.errors.SchemaErrors as err:
        logger.error(f"Schema validation FAILED for {df_name}.")
        try:
            error_details = err.failure_cases.to_dict(orient='records')
            logger.error(f"Validation Failure Cases (sample):\n{json.dumps(error_details[:5], indent=2, default=str)}")
            validation_errors = err.failure_cases
        except Exception as json_err:
            logger.error(f"Could not serialize validation errors: {json_err}")
            logger.error(f"Pandera failure cases head:\n{err.failure_cases.head()}")
            validation_errors = err.failure_cases
        logger.warning(f"Proceeding with checks on *original* {df_name} DataFrame.")
        validated_df = df # Use original for reporting
    except Exception as verr:
        logger.error(f"Unexpected error during Pandera validation for {df_name}: {verr}\n{traceback.format_exc()}")
        validated_df = df

    report = {'dataframe_name': df_name, 'total_rows': len(validated_df), 'total_columns': len(validated_df.columns),
              'schema_validation_passed': validation_errors is None,
              'validation_error_count': len(validation_errors) if validation_errors is not None else 0}
    missing_counts = validated_df.isnull().sum()
    report['missing_values_summary'] = json.dumps(missing_counts[missing_counts > 0].apply(int).to_dict())

    numeric_cols = validated_df.select_dtypes(include=['number']).columns
    stats_summary = {}
    for col in numeric_cols:
        try:
            desc = validated_df[col].dropna().describe().to_dict()
            stats_summary[col] = {k: round(v, 2) if isinstance(v, (int, float)) else v for k, v in desc.items()}
        except Exception as e: stats_summary[col] = f"Error: {e}"
    report['numeric_column_stats'] = json.dumps(stats_summary, default=str)

    json_string_cols = [c for c in validated_df.columns if c.endswith('_json')]
    json_len_summary = {}
    for col in json_string_cols:
        try:
             if col in validated_df and not validated_df[col].dropna().empty:
                 lengths = validated_df[col].apply(safe_json_load).apply(len)
                 json_len_summary[col] = {'mean_items': round(lengths.mean(), 2), 'max_items': int(lengths.max()), 'total_items': int(lengths.sum())}
             else: json_len_summary[col] = {'mean_items': 0, 'max_items': 0, 'total_items': 0}
        except Exception as e: json_len_summary[col] = f"Error: {e}"
    report['json_string_column_stats'] = json.dumps(json_len_summary, default=str)

    # Use profile_id from main_profiles, review_id from reviews
    pk_col = "profile_id" if df_name == "main_profiles" else "review_id" if df_name == "reviews" else None
    if pk_col and pk_col in validated_df.columns:
         # Check for nulls before checking duplicates
         null_pks = validated_df[pk_col].isnull().sum()
         if null_pks > 0: logger.warning(f"Found {null_pks} null primary keys ('{pk_col}') in {df_name}.")
         num_duplicates = int(validated_df[pk_col].dropna().duplicated().sum())
         report['duplicate_pk_rows'] = num_duplicates
         if num_duplicates > 0: logger.warning(f"Found {num_duplicates} duplicate non-null PKs ('{pk_col}') in {df_name}.")
    elif df_name == "nested_profiles" and "profile_id" in validated_df.columns and "sub_profile_type" in validated_df.columns:
         # Check composite key for nested profiles
         num_duplicates = int(validated_df.duplicated(subset=["profile_id", "sub_profile_type"], keep=False).sum())
         report['duplicate_pk_rows'] = num_duplicates
         if num_duplicates > 0: logger.warning(f"Found {num_duplicates} duplicate composite PKs (profile_id, sub_profile_type) in {df_name}.")
    else: report['duplicate_pk_rows'] = 'N/A'

    try:
        ensure_dir_exists(output_dir)
        report_df = pd.DataFrame([report])
        report_file = output_dir / f"quality_report_{df_name}.csv"
        report_df.to_csv(report_file, index=False, encoding='utf-8')
        logger.info(f"Quality report saved to: {report_file}")
        output_data_file = output_dir / f"{df_name}_processed.csv"
        # Use validated_df if validation passed, otherwise original df might be more complete
        df_to_save = validated_df if validation_errors is None else df
        df_to_save.to_csv(output_data_file, index=False, encoding='utf-8')
        logger.info(f"Processed data saved to: {output_data_file}")
        if validation_errors is not None:
            error_file = output_dir / f"validation_errors_{df_name}.csv"
            validation_errors.to_csv(error_file, index=False, encoding='utf-8')
            logger.info(f"Validation errors saved to: {error_file}")
    except Exception as save_err:
        logger.error(f"Failed to save report or data for {df_name}: {save_err}\n{traceback.format_exc()}")

    logger.info(f"--- Finished Quality Checks for {df_name} ---")
    return validated_df # Return the validated df

def print_proposed_data_model():
    """Prints the proposed relational data model (hybrid flattening)."""
    model = """
    Proposed Relational Data Model (Hybrid Flattening):
    ----------------------------------------------------
    NOTE: NestedServiceProfiles columns contain dynamically flattened fields
          from qualities, services, etc. prefixed by sub-profile type.
          JSON columns require further parsing/normalization in ETL/DB.

    1. Profiles (Main Caregiver Information)
       - profile_id (PK, VARCHAR) - From member.id
       - source_filename (VARCHAR)
       - member_firstName (VARCHAR, Nullable)
       - member_lastName (VARCHAR, Nullable)
       - member_gender (VARCHAR, Nullable)
       - member_displayName (VARCHAR, Nullable)
       - member_email (VARCHAR, Nullable)
       - member_primaryService (VARCHAR, Nullable)
       - member_hiResImageURL (VARCHAR, Nullable)
       - member_imageURL (VARCHAR, Nullable)
       - member_address_city (VARCHAR, Nullable)
       - member_address_state (VARCHAR, Nullable)
       - member_address_zip (VARCHAR, Nullable)
       - member_languages_json (JSONB/TEXT)
       - member_legacyId (VARCHAR, Nullable)
       - member_isPremium (BOOLEAN, Nullable)
       - distanceFromSeekerInMiles (FLOAT, Nullable)
       - hasCareCheck (BOOLEAN, Nullable)
       - hasGrantedCriminalBGCAccess (BOOLEAN, Nullable)
       - hasGrantedMvrBGCAccess (BOOLEAN, Nullable)
       - hasGrantedPremierBGCAccess (BOOLEAN, Nullable)
       - hiredTimes (INTEGER, Nullable)
       - isFavorite (BOOLEAN, Nullable)
       - isMVREligible (BOOLEAN, Nullable)
       - isVaccinated (BOOLEAN, Nullable)
       - providerStatus (VARCHAR, Nullable)
       - responseRate (INTEGER, Nullable)
       - responseTime (INTEGER, Nullable)
       - signUpDate (TIMESTAMP, Nullable)
       - yearsOfExperience (INTEGER, Nullable)
       - badges_json (JSONB/TEXT)
       - backgroundChecks_json (JSONB/TEXT)
       - educationDegrees_json (JSONB/TEXT)
       - hiredByCounts_json (JSONB/TEXT)
       - placeInfo_json (JSONB/TEXT)
       - recurringAvailability_json (JSONB/TEXT)
       - nonPrimaryImages_json (JSONB/TEXT)
       - continuousBackgroundCheck_json (JSONB/TEXT)
       # Note: profiles_json is used for processing but typically not stored directly

    2. NestedServiceProfiles (Explicit top-level, dynamic inner fields)
       - nested_profile_pk (PK, SERIAL or UUID - Added during ETL)
       - profile_id (FK -> Profiles.profile_id)
       - sub_profile_type (VARCHAR) - e.g., 'commonCaregiverProfile', 'houseKeepingCaregiverProfile'
       - service_ids_json (JSONB/TEXT)
       # --- Explicit Simple Sub-Profile Fields (Examples) ---
       - commonCaregiverProfile_id (VARCHAR, Nullable)
       - commonCaregiverProfile_repeatClientsCount (INTEGER, Nullable)
       - seniorCareCaregiverProfile_id (VARCHAR, Nullable)
       - seniorCareCaregiverProfile_approvalStatus (VARCHAR, Nullable)
       - seniorCareCaregiverProfile_availabilityFrequency (VARCHAR, Nullable)
       - seniorCareCaregiverProfile_yearsOfExperience (INTEGER, Nullable)
       - houseKeepingCaregiverProfile_id (VARCHAR, Nullable)
       - houseKeepingCaregiverProfile_approvalStatus (VARCHAR, Nullable)
       - houseKeepingCaregiverProfile_yearsOfExperience (INTEGER, Nullable)
       - houseKeepingCaregiverProfile_distanceWillingToTravel (INTEGER, Nullable)
       - childCareCaregiverProfile_id (VARCHAR, Nullable)
       - childCareCaregiverProfile_numberOfChildren (INTEGER, Nullable)
       # --- Dynamically Flattened Inner Fields (Examples) ---
       - commonCaregiverProfile_merchandizedJobInterests_companionCare (BOOLEAN, Nullable)
       - commonCaregiverProfile_merchandizedJobInterests_lightCleaning (BOOLEAN, Nullable)
       - seniorCareCaregiverProfile_bio_experienceSummary (TEXT, Nullable)
       - seniorCareCaregiverProfile_payRange_hourlyRateFrom_amount (VARCHAR -> NUMERIC, Nullable)
       - seniorCareCaregiverProfile_qualities_cprTrained (BOOLEAN, Nullable)
       - seniorCareCaregiverProfile_supportedServices_transportation (BOOLEAN, Nullable)
       - houseKeepingCaregiverProfile_qualities_doesNotSmoke (BOOLEAN, Nullable)
       - houseKeepingCaregiverProfile_supportedServices_laundry (BOOLEAN, Nullable)
       # --- JSON Lists within Sub-Profiles (Examples) ---
       - seniorCareCaregiverProfile_otherQualities_json (JSONB/TEXT)
       - childCareCaregiverProfile_ageGroups_json (JSONB/TEXT)
       - childCareCaregiverProfile_rates_json (JSONB/TEXT)
       - houseKeepingCaregiverProfile_schedule_json (JSONB/TEXT)

    3. Reviews (Explicit Fields)
       - review_id (PK, VARCHAR)
       - profile_id (FK -> Profiles.profile_id, Nullable)
       - source_filename (VARCHAR)
       - careType (VARCHAR, Nullable)
       - createTime (TIMESTAMP, Nullable)
       - deleteTime (TIMESTAMP, Nullable)
       - description_displayText (TEXT, Nullable)
       - description_originalText (TEXT, Nullable)
       - languageCode (VARCHAR, Nullable)
       - originalSource (VARCHAR, Nullable)
       - status (VARCHAR, Nullable)
       - updateSource (VARCHAR, Nullable)
       - updateTime (TIMESTAMP, Nullable)
       - verifiedByCare (BOOLEAN, Nullable)
       - reviewee_providerType (VARCHAR, Nullable)
       - reviewee_type (VARCHAR, Nullable)
       - reviewer_imageURL (VARCHAR, Nullable)
       - reviewer_firstName (VARCHAR, Nullable)
       - reviewer_lastInitial (VARCHAR, Nullable)
       - reviewer_source (VARCHAR, Nullable)
       - reviewer_type (VARCHAR, Nullable)
       - ratings_json (JSONB/TEXT) -> Normalize to ReviewRatings
       - retort_json (JSONB/TEXT)
       - attributes_json (JSONB/TEXT) -> Normalize to ReviewAttributes

    Normalization Strategy Remains Similar:
    - Normalize JSON fields (languages, badges, checks, degrees, ratings, attributes, lists within nested profiles)
      into separate tables during the database loading (ETL) phase.
    """
    print(model)

# --- Main Execution ---
if __name__ == "__main__":
    logger.info(f"Starting Data Quality Check Script for {COUNTRY}/{EXAMPLE_ZIP}")
    # Ensure output dirs exist
    ensure_dir_exists(BASE_PREPROCESSED_DIR)
    ensure_dir_exists(COUNTRY_PREPROCESSED_DIR)
    ensure_dir_exists(ZIP_PREPROCESSED_DIR)

    main_profiles_df = pd.DataFrame()
    nested_profiles_df = pd.DataFrame()
    reviews_df = pd.DataFrame()

    # --- Process Profiles ---
    logger.info(f"--- Processing Profiles ---")
    logger.info(f"Input Dir: {PROFILES_INPUT_DIR}")
    logger.info(f"Output Dir: {PROFILES_OUTPUT_DIR}")
    profile_json_files = load_json_files(PROFILES_INPUT_DIR)

    if profile_json_files:
        main_profiles_df = create_dataframe(profile_json_files, flatten_main_profile, "main_profiles")
        if not main_profiles_df.empty:
            main_profiles_df = run_quality_checks_and_report(main_profiles_df, main_profile_schema, "main_profiles", PROFILES_OUTPUT_DIR)
            nested_profile_rows = []
            for idx, row in main_profiles_df.iterrows():
                 # Defend against potential errors in a single row's processing
                 try:
                    nested_profile_rows.extend(extract_and_flatten_nested_profiles(row))
                 except Exception as nested_err:
                    logger.error(f"Error during nested profile extraction for profile_id {row.get('profile_id','N/A')} from {row.get('source_filename','N/A')}: {nested_err}\n{traceback.format_exc()}")

            if nested_profile_rows:
                nested_profiles_df = pd.DataFrame(nested_profile_rows)
                nested_profiles_df = nested_profiles_df.convert_dtypes()
                nested_profiles_df = run_quality_checks_and_report(nested_profiles_df, nested_profile_schema, "nested_profiles", PROFILES_OUTPUT_DIR)
            else: logger.warning("No nested profile data was extracted or survived processing.")
        else: logger.warning("Main profiles DataFrame is empty after flattening.")
    else: logger.warning(f"No profile JSON files found. Skipping profile processing.")

    # --- Process Reviews ---
    logger.info(f"--- Processing Reviews ---")
    logger.info(f"Input Dir: {REVIEWS_INPUT_DIR}")
    logger.info(f"Output Dir: {REVIEWS_OUTPUT_DIR}")
    review_json_files = []
    if REVIEWS_INPUT_DIR.is_dir():
        for type_dir in REVIEWS_INPUT_DIR.iterdir():
            if type_dir.is_dir():
                logger.info(f"Loading reviews from type directory: {type_dir.name}")
                review_json_files.extend(load_json_files(type_dir))
    else: logger.warning(f"Reviews input directory does not exist: {REVIEWS_INPUT_DIR}")

    if review_json_files:
        reviews_df = create_reviews_dataframe(review_json_files)
        if not reviews_df.empty:
            reviews_df = run_quality_checks_and_report(reviews_df, review_schema, "reviews", REVIEWS_OUTPUT_DIR)
        else: logger.warning("Reviews DataFrame is empty after processing.")
    else: logger.warning(f"No review JSON files found. Skipping review processing.")

    logger.info("--- Proposed Data Model ---")
    print_proposed_data_model()
    logger.info("--- Data Quality Check Script Finished ---")
    
def run_quality():
    logger.info(f"Starting Data Quality Check Script for {COUNTRY}/{EXAMPLE_ZIP}")
    # Ensure output dirs exist
    ensure_dir_exists(BASE_PREPROCESSED_DIR)
    ensure_dir_exists(COUNTRY_PREPROCESSED_DIR)
    ensure_dir_exists(ZIP_PREPROCESSED_DIR)

    main_profiles_df = pd.DataFrame()
    nested_profiles_df = pd.DataFrame()
    reviews_df = pd.DataFrame()

    # --- Process Profiles ---
    logger.info(f"--- Processing Profiles ---")
    logger.info(f"Input Dir: {PROFILES_INPUT_DIR}")
    logger.info(f"Output Dir: {PROFILES_OUTPUT_DIR}")
    profile_json_files = load_json_files(PROFILES_INPUT_DIR)

    if profile_json_files:
        main_profiles_df = create_dataframe(profile_json_files, flatten_main_profile, "main_profiles")
        if not main_profiles_df.empty:
            main_profiles_df = run_quality_checks_and_report(main_profiles_df, main_profile_schema, "main_profiles", PROFILES_OUTPUT_DIR)
            nested_profile_rows = []
            for idx, row in main_profiles_df.iterrows():
                 # Defend against potential errors in a single row's processing
                 try:
                    nested_profile_rows.extend(extract_and_flatten_nested_profiles(row))
                 except Exception as nested_err:
                    logger.error(f"Error during nested profile extraction for profile_id {row.get('profile_id','N/A')} from {row.get('source_filename','N/A')}: {nested_err}\n{traceback.format_exc()}")

            if nested_profile_rows:
                nested_profiles_df = pd.DataFrame(nested_profile_rows)
                nested_profiles_df = nested_profiles_df.convert_dtypes()
                nested_profiles_df = run_quality_checks_and_report(nested_profiles_df, nested_profile_schema, "nested_profiles", PROFILES_OUTPUT_DIR)
            else: logger.warning("No nested profile data was extracted or survived processing.")
        else: logger.warning("Main profiles DataFrame is empty after flattening.")
    else: logger.warning(f"No profile JSON files found. Skipping profile processing.")

    # --- Process Reviews ---
    logger.info(f"--- Processing Reviews ---")
    logger.info(f"Input Dir: {REVIEWS_INPUT_DIR}")
    logger.info(f"Output Dir: {REVIEWS_OUTPUT_DIR}")
    review_json_files = []
    if REVIEWS_INPUT_DIR.is_dir():
        for type_dir in REVIEWS_INPUT_DIR.iterdir():
            if type_dir.is_dir():
                logger.info(f"Loading reviews from type directory: {type_dir.name}")
                review_json_files.extend(load_json_files(type_dir))
    else: logger.warning(f"Reviews input directory does not exist: {REVIEWS_INPUT_DIR}")

    if review_json_files:
        reviews_df = create_reviews_dataframe(review_json_files)
        if not reviews_df.empty:
            reviews_df = run_quality_checks_and_report(reviews_df, review_schema, "reviews", REVIEWS_OUTPUT_DIR)
        else: logger.warning("Reviews DataFrame is empty after processing.")
    else: logger.warning(f"No review JSON files found. Skipping review processing.")

    logger.info("--- Proposed Data Model ---")
    print_proposed_data_model()
    logger.info("--- Data Quality Check Script Finished ---")