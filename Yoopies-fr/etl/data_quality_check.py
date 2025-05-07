# src/etl/yoopies_pre_etl_processor_v2.py (Renamed for clarity)

import os
import json
import logging
import traceback
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

# --- Configuration ---
BASE_RAW_DIR = Path("data/raw/yoopies")
BASE_PREPROCESSED_DIR = Path("preprocessed_data/yoopies")

# --- Logging Setup ---
LOG_FILE = (
    BASE_PREPROCESSED_DIR / f"data_quality_aggregation_normalization_v2.log"
)  # New log file


def ensure_dir_exists(path: Path):
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {path}")
        except Exception as e:
            print(f"FATAL: Could not create directory {path}: {e}")
            raise


ensure_dir_exists(BASE_PREPROCESSED_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# --- Helper Functions (safe_json_dump, safe_json_load, extract_context_from_path, load_all_page_files - unchanged) ---
def safe_json_dump(data, default="null"):
    try:
        return json.dumps(data, default=str)
    except TypeError:
        return default


def safe_json_load(json_string, return_type="list"):
    default_return = (
        [] if return_type == "list" else {} if return_type == "dict" else None
    )
    if pd.isna(json_string) or not isinstance(json_string, str):
        return default_return
    try:
        loaded = json.loads(json_string)
        if return_type == "list" and not isinstance(loaded, list):
            return [loaded] if isinstance(loaded, (str, int, float, dict)) else []
        if return_type == "dict" and not isinstance(loaded, dict):
            return {}
        return loaded
    except (json.JSONDecodeError, TypeError):
        return default_return


def extract_context_from_path(file_path: Path) -> Dict[str, str]:
    try:
        relative_path = file_path.relative_to(BASE_RAW_DIR)
        parts = relative_path.parts
        if len(parts) >= 4:
            return {
                "postal_code": parts[0],
                "care_type": parts[1],
                "sub_type": parts[2],
            }
        else:
            logger.warning(
                f"Unexpected path structure for context extraction: {relative_path}"
            )
            return {
                "postal_code": "unknown",
                "care_type": "unknown",
                "sub_type": "unknown",
            }
    except ValueError:
        logger.error(f"Could not determine relative path for context: {file_path}")
        return {"postal_code": "unknown", "care_type": "unknown", "sub_type": "unknown"}


def load_all_page_files(base_directory: Path) -> List[Dict[str, Any]]:
    json_items = []
    if not base_directory.is_dir():
        logger.error(f"Base input directory not found: {base_directory}")
        return json_items
    logger.info(f"Scanning for page_*.json files recursively in: {base_directory}")
    total_files_found = 0
    loaded_count = 0
    for file_path in base_directory.rglob("page_*.json"):
        total_files_found += 1
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                page_content = json.load(f)
            context = extract_context_from_path(file_path)
            item = {
                "filename": str(file_path.relative_to(BASE_RAW_DIR)),
                **context,
                "page_data": page_content,
            }
            json_items.append(item)
            loaded_count += 1
        except Exception as e:
            logger.error(f"Error reading/processing file {file_path}: {e}")
    logger.info(
        f"Scanned {total_files_found} potential page files. Loaded {loaded_count} files."
    )
    return json_items


# --- Flattening Function (Unchanged) ---
def flatten_yoopies_ad(ad_edge: dict, context: dict) -> dict:
    flat_ad = {**context, "_flattening_error": False}  # Use context directly
    #del flat_ad["page_data"]  # Remove raw page data from context if present

    try:
        node = ad_edge.get("node", {})
        ad = node.get("ad", {})
        user = ad.get("user", {}) if isinstance(ad, dict) else {}

        if not ad:
            flat_ad["_flattening_error"] = True
            return flat_ad
        ad_id = ad.get("id")
        if ad_id is None:
            flat_ad["_flattening_error"] = True
            return flat_ad
        flat_ad["ad_id"] = ad_id

        # Ad Level
        flat_ad.update(
            {
                k: ad.get(k)
                for k in [
                    "title",
                    "content",
                    "type",
                    "status",
                    "createdAt",
                    "updatedAt",
                    "experienceAmount",
                ]
            }
        )
        flat_ad.update(
            {
                "slug_title": ad.get("slug", {}).get("title"),
                "slug_city": ad.get("slug", {}).get("city"),
                "address_latitude": ad.get("address", {}).get("latitude"),
                "address_longitude": ad.get("address", {}).get("longitude"),
                "address_city": ad.get("address", {}).get("city"),
                "address_cityKey": ad.get("address", {}).get("cityKey"),
                "address_country": ad.get("address", {}).get("country"),
                "address_zipCode": ad.get("address", {}).get("zipCode"),
                "categories_json": safe_json_dump(ad.get("categories", [])),
                "badges_json": safe_json_dump(ad.get("badges", [])),
                "employmentTypes_json": safe_json_dump(ad.get("employmentTypes", [])),
                "tags_json": safe_json_dump(ad.get("tags", {})),
                "sitter_parentAdType_json": safe_json_dump(ad.get("parentAdType", [])),
                "sitter_childminderSlots_json": safe_json_dump(
                    ad.get("childminderSlots", {})
                ),
            }
        )

        # User Level
        if user:
            flat_ad.update(
                {
                    k: user.get(k)
                    for k in [
                        "id",
                        "enabled",
                        "baseType",
                        "isVerified",
                        "firstName",
                        "lastName",
                        "hasAEStatus",
                        "age",
                    ]
                }
            )
            flat_ad.update(
                {  # Rename user fields
                    "user_id": flat_ad.pop("id", None),
                    "user_enabled": flat_ad.pop("enabled", None),
                    "user_baseType": flat_ad.pop("baseType", None),
                    "user_isVerified": flat_ad.pop("isVerified", None),
                    "user_firstName": flat_ad.pop("firstName", None),
                    "user_lastName": flat_ad.pop("lastName", None),
                    "user_applicant_hasAEStatus": flat_ad.pop("hasAEStatus", None),
                    "user_applicant_age": flat_ad.pop("age", None),
                }
            )
            flat_ad.update(
                {
                    "user_photos_thumbnail": user.get("photos", {}).get("thumbnail"),
                    "user_photos_large": user.get("photos", {}).get("large"),
                    "user_photos_extra_large": user.get("photos", {}).get(
                        "extra_large"
                    ),
                    "user_roles_json": safe_json_dump(user.get("roles", [])),
                    "user_grades_json": safe_json_dump(user.get("grades", {})),
                    "user_options_json": safe_json_dump(user.get("options", {})),
                    "user_threadsConnections_json": safe_json_dump(
                        user.get("threadsConnections", {})
                    ),
                }
            )
            grades_data = user.get("grades", {})
            flat_ad["user_grades_count"] = (
                grades_data.get("count") if isinstance(grades_data, dict) else None
            )
            flat_ad["user_grades_average"] = (
                grades_data.get("average") if isinstance(grades_data, dict) else None
            )
        else:
            user_fields = [
                "user_id",
                "user_enabled",
                "user_baseType",
                "user_isVerified",
                "user_firstName",
                "user_lastName",
                "user_photos_thumbnail",
                "user_photos_large",
                "user_photos_extra_large",
                "user_applicant_hasAEStatus",
                "user_applicant_age",
                "user_grades_count",
                "user_grades_average",
                "user_roles_json",
                "user_grades_json",
                "user_options_json",
                "user_threadsConnections_json",
            ]
            for field in user_fields:
                flat_ad[field] = None if not field.endswith("_json") else "null"

        # Type Conversion & Cleaning
        #for ts_field in ["ad_createdAt", "ad_updatedAt"]:
        for ts_field in ["createdAt", "updatedAt"]:
            flat_ad[ts_field] = pd.to_datetime(flat_ad.get(ts_field), errors="coerce")
        for bool_field in [
            "user_enabled",
            "user_isVerified",
            "user_applicant_hasAEStatus",
        ]:
            val = flat_ad.get(bool_field)
            flat_ad[bool_field] = None if val is None else bool(val)
        num_fields = [
            "address_latitude",
            "address_longitude",
            "user_applicant_age",
            "user_grades_count",
            "user_grades_average",
        ]
        for field in num_fields:
            val = flat_ad.get(field)
            flat_ad[field] = pd.to_numeric(val, errors="coerce")
        for str_field in [
            "title",  #
            "content", #
            "user_firstName",
            "user_lastName",
            "address_city",
            "address_zipCode",
        ]:
            val = flat_ad.get(str_field)
            flat_ad[str_field] = val.strip() if isinstance(val, str) else val

        # Rename ad fields for clarity
        rename_map = {
            k: f"ad_{k}"
            for k in [
                "title",
                "content",
                "type",
                "status",
                "createdAt",
                "updatedAt",
                "experienceAmount",
            ]
        }
        flat_ad = {rename_map.get(k, k): v for k, v in flat_ad.items()}

        return flat_ad

    except Exception as e:
        logger.error(
            f"Flattening error ad_id {flat_ad.get('ad_id', 'N/A')}: {e}", exc_info=False
        )  # Reduce traceback noise
        flat_ad["_flattening_error"] = True
        return flat_ad


# --- DataFrame Creation (Unchanged) ---
# --- DataFrame Creation (from all loaded pages) ---
def create_aggregated_dataframe(page_items: list) -> pd.DataFrame:
    """
    Creates a single aggregated DataFrame from all loaded page items.
    """
    flattened_data = []; skipped_edges = 0; processed_edges = 0; skipped_items = 0

    logger.info(f"Aggregating data from {len(page_items)} loaded page files...")

    for item in page_items:
        # --- Robustness Check ---
        if not isinstance(item, dict):
            logger.warning(f"Skipping item because it is not a dictionary: {type(item)}")
            skipped_items += 1
            continue
        if 'filename' not in item or 'page_data' not in item:
            logger.warning(f"Skipping item due to missing 'filename' or 'page_data' key. Keys found: {list(item.keys())}")
            skipped_items += 1
            continue
        # --- End Robustness Check ---

        filename = item.get("filename", "unknown") # Now safer to access
        page_context = {k: v for k, v in item.items() if k != 'page_data'}
        page_data = item.get('page_data', {}) # .get() is still safe

        try:
            # Navigate to the list of ads (edges)
            ads_section = page_data.get("data", {}).get("ads", {})
            if not isinstance(ads_section, dict):
                logger.debug(f"No 'data.ads' dict found in {filename}.") # Optional debug log
                continue
            ad_edges = ads_section.get("edges", [])
            if not isinstance(ad_edges, list):
                logger.debug(f"No 'data.ads.edges' list found in {filename}.") # Optional debug log
                continue

            if not ad_edges:
                logger.debug(f"Empty 'edges' list in {filename}.") # Optional debug log
                continue

            # Iterate through each ad (edge) in the page
            for edge in ad_edges:
                if isinstance(edge, dict):
                    flat_item = flatten_yoopies_ad(edge, page_context)
                    if isinstance(flat_item, dict) and not flat_item.get("_flattening_error"):
                        flattened_data.append(flat_item)
                        processed_edges += 1
                    else:
                        skipped_edges += 1
                else:
                    logger.warning(f"Skipping non-dict item found in 'edges' list in {filename}.")
                    skipped_edges += 1

        except Exception as e: # Catch any other unexpected errors during processing
            logger.error(f"Error processing page file {filename} for aggregation: {e}", exc_info=True)
            # Continue to the next file item even if one file fails internally
            continue

    if skipped_items > 0:
         logger.warning(f"Skipped {skipped_items} items due to structural issues (not dict or missing keys).")
    if skipped_edges > 0:
        logger.warning(f"{skipped_edges} ad records failed to flatten or were skipped during aggregation.")

    if not flattened_data:
        logger.warning("No valid ad data aggregated.")
        return pd.DataFrame()

    df = pd.DataFrame(flattened_data)
    if "_flattening_error" in df.columns:
        df = df.drop(columns=["_flattening_error"])

    # Use pandas convert_dtypes for better type inference
    try:
        df = df.convert_dtypes()
    except Exception as e:
        logger.error(f"Error during convert_dtypes: {e}. Proceeding with potentially incorrect types.")


    logger.info(
        f"Created aggregated DataFrame with {len(df)} records ({processed_edges} processed edges) "
        f"and {len(df.columns)} columns."
    )
    return df


# --- Pandera Schemas ---
# FIX 1: Update Check functions using pd.isna()
reasonable_age_flat = Check(
    lambda s: pd.isna(s) | ((s >= 16) & (s <= 99)),
    element_wise=True,
    error="Age out of range (16-99)",
)
valid_grade_flat = Check(
    lambda s: pd.isna(s) | ((s >= 0) & (s <= 5)),
    element_wise=True,
    error="Grade out of range (0-5)",
)
non_negative_count_flat = Check(
    lambda s: pd.isna(s) | (s >= 0), element_wise=True, error="Count cannot be negative"
)

# Schema for flattened data (use updated checks)
flattened_ad_schema = DataFrameSchema(
    {
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "user_id": Column(pd.Int64Dtype(), nullable=True),
        "source_filename": Column(str, nullable=False),
        "postal_code": Column(str, nullable=False),
        "care_type": Column(str, nullable=False),
        "sub_type": Column(str, nullable=False),
        "ad_title": Column(str, nullable=True, coerce=True),
        "ad_type": Column(str, nullable=True, coerce=True),
        "ad_status": Column(
            str,
            nullable=True,
            coerce=True,
            checks=Check.isin(["ACTIVE", "INACTIVE", "PENDING", None]),
        ),
        "ad_createdAt": Column("datetime64[ns]", nullable=True, coerce=True),
        "ad_updatedAt": Column("datetime64[ns]", nullable=True, coerce=True),
        "user_enabled": Column(pd.BooleanDtype(), nullable=True),
        "user_isVerified": Column(pd.BooleanDtype(), nullable=True),
        "user_applicant_age": Column(
            pd.Int64Dtype(), nullable=True, checks=reasonable_age_flat
        ),  # Use updated check
        "user_grades_count": Column(
            pd.Int64Dtype(), nullable=True, checks=non_negative_count_flat
        ),  # Use updated check
        "user_grades_average": Column(
            float, nullable=True, coerce=True, checks=valid_grade_flat
        ),  # Use updated check
        "categories_json": Column(str, nullable=True, coerce=True),
        "badges_json": Column(str, nullable=True, coerce=True),
        "employmentTypes_json": Column(str, nullable=True, coerce=True),
        "tags_json": Column(str, nullable=True, coerce=True),
        "user_roles_json": Column(str, nullable=True, coerce=True),
        "user_grades_json": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
    unique=["ad_id"],
)

# Schemas for NORMALIZED Tables
ads_schema = DataFrameSchema(
    {
        "ad_id": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "user_id": Column(pd.Int64Dtype(), nullable=True),
        "postal_code": Column(str, nullable=False),
        "care_type": Column(str, nullable=False),
        "sub_type": Column(str, nullable=False),
        "ad_title": Column(str, nullable=True),
        "ad_content": Column(str, nullable=True),
        "ad_type": Column(str, nullable=True),
        "ad_status": Column(str, nullable=True),
        "ad_createdAt": Column("datetime64[ns]", nullable=True),
        "ad_updatedAt": Column("datetime64[ns]", nullable=True),
        "ad_experienceAmount": Column(str, nullable=True),
        "address_latitude": Column(float, nullable=True),
        "address_longitude": Column(float, nullable=True),
        "address_city": Column(str, nullable=True),
        "address_zipCode": Column(str, nullable=True),
    },
    strict=True,
    coerce=True,
)

users_schema = DataFrameSchema(
    {
        "user_id": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "user_enabled": Column(pd.BooleanDtype(), nullable=True),
        "user_baseType": Column(str, nullable=True),
        "user_isVerified": Column(pd.BooleanDtype(), nullable=True),
        "user_firstName": Column(str, nullable=True),
        "user_lastName": Column(str, nullable=True),
        "user_applicant_hasAEStatus": Column(pd.BooleanDtype(), nullable=True),
        "user_applicant_age": Column(
            pd.Int64Dtype(), nullable=True, checks=reasonable_age_flat
        ),  # Use updated check
        "user_grades_count": Column(
            pd.Int64Dtype(), nullable=True, checks=non_negative_count_flat
        ),  # Use updated check
        "user_grades_average": Column(
            float, nullable=True, checks=valid_grade_flat
        ),  # Use updated check
    },
    strict=True,
    coerce=True,
)

ad_categories_schema = DataFrameSchema(
    {
        "ad_category_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "service": Column(str, nullable=True),
        "category": Column(str, nullable=True),
    },
    strict=True,
    coerce=True,
)

ad_badges_schema = DataFrameSchema(
    {
        "ad_badge_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "badge_name": Column(str, nullable=False),
        "badge_additionalData": Column(str, nullable=True),
    },
    strict=True,
    coerce=True,
)

ad_rates_schema = DataFrameSchema(
    {
        "ad_rate_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "employment_type": Column(str, nullable=True),
        "care_type": Column(str, nullable=True),
        "rate_type": Column(str, nullable=True),
        "rate_amount": Column(float, nullable=True),
        "rate_timeUnit": Column(str, nullable=True),
        "rate_commission": Column(float, nullable=True),
    },
    strict=True,
    coerce=True,
)

ad_tags_schema = DataFrameSchema(
    {
        "ad_tag_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "tag_type": Column(str, nullable=False),
        "tag_value": Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)

ad_languages_schema = DataFrameSchema(
    {
        "ad_language_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "language_code": Column(str, nullable=True),  # FIX 3: Allow nulls
        "language_index": Column(pd.Int64Dtype(), nullable=True),
    },
    strict=True,
    coerce=True,
)

user_roles_schema = DataFrameSchema(
    {  # Schema for the new function's output
        "user_role_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "user_id": Column(pd.Int64Dtype(), nullable=False),
        "role_name": Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)

reviews_schema = DataFrameSchema(
    {
        "review_pk": Column(pd.Int64Dtype(), nullable=False, unique=True),
        "ad_id": Column(pd.Int64Dtype(), nullable=False),
        "reviewed_user_id": Column(pd.Int64Dtype(), nullable=True),
        "reviewer_firstName": Column(str, nullable=True),
        "reviewer_lastName": Column(str, nullable=True),
        "review_content": Column(str, nullable=True),
        "review_generalGrade": Column(
            pd.Int64Dtype(), nullable=True, checks=Check.isin([0, 1, 2, 3, 4, 5, None])
        ),
        "review_experienceName": Column(str, nullable=True),
    },
    strict=True,
    coerce=True,
)


# --- Quality Check Function (Updated logging for empty DFs) ---
def run_quality_checks_and_report(
    df: pd.DataFrame, schema: DataFrameSchema, df_name: str, output_dir: Path
) -> Tuple[pd.DataFrame, bool]:
    logger.info(f"--- Running Quality Checks for {df_name} ({len(df)} rows) ---")
    is_empty = df.empty
    if is_empty:
        logger.warning(f"DataFrame '{df_name}' is empty. Skipping checks.")
        report = {"dataframe_name": df_name, "total_rows": 0, "status": "empty"}
        report_df = pd.DataFrame([report])
        report_file = output_dir / f"quality_report_{df_name}.csv"
        try:
            report_df.to_csv(report_file, index=False, encoding="utf-8")
            logger.info(f"Saved empty data report to: {report_file}")
        except Exception as e:
            logger.error(f"Failed to save empty report for {df_name}: {e}")
        return df, False  # Indicate failure/empty

    validated_df = df.copy()
    validation_errors_df = None
    validation_passed = False
    validation_error_count = 0
    validated_df_for_stats = df.copy()  # Use original for stats if validation fails

    try:
        validated_df = schema.validate(validated_df, lazy=True)
        logger.info(f"Schema validation PASSED for {df_name}.")
        validation_passed = True
        validated_df_for_stats = validated_df.copy()  # Use validated for stats
    except pa.errors.SchemaErrors as err:
        logger.error(f"Schema validation FAILED for {df_name}.")
        validation_errors_df = err.failure_cases
        validation_error_count = len(validation_errors_df)
        try:  # Log sample errors safely
            error_sample = validation_errors_df.head().to_dict(orient="records")
            logger.error(
                f"Validation Errors ({validation_error_count}) Sample:\n{json.dumps(error_sample, indent=2, default=str)}"
            )
        except Exception:
            logger.error("Could not display validation error sample.")
    except Exception as verr:
        logger.error(
            f"Unexpected Pandera validation error for {df_name}: {verr}", exc_info=True
        )
        validation_error_count = -1  # System error

    # --- Quality Metrics Calculation ---
    report = {
        "dataframe_name": df_name,
        "total_rows": len(validated_df_for_stats),
        "total_columns": len(validated_df_for_stats.columns),
        "schema_validation_passed": validation_passed,
        "validation_error_count": validation_error_count,
    }
    missing_counts = validated_df_for_stats.isnull().sum()
    report["missing_values_total"] = int(missing_counts.sum())
    report["missing_values_summary_json"] = safe_json_dump(
        missing_counts[missing_counts > 0].astype(int).to_dict()
    )
    report["data_types_summary_json"] = safe_json_dump(
        validated_df_for_stats.dtypes.astype(str).value_counts().to_dict()
    )

    # PK Checks
    pk_col = next(
        (c for c in df.columns if c.endswith("_pk") or c in ["ad_id", "user_id"]), None
    )
    if pk_col:
        null_pks = validated_df_for_stats[pk_col].isnull().sum()
        report["primary_key_null_count"] = int(null_pks)
        num_duplicates = int(validated_df_for_stats[pk_col].dropna().duplicated().sum())
        report["duplicate_pk_rows"] = num_duplicates
        if null_pks > 0:
            logger.warning(f"{null_pks} null PKs ('{pk_col}') in {df_name}.")
        if num_duplicates > 0:
            logger.warning(f"{num_duplicates} duplicate PKs ('{pk_col}') in {df_name}.")
    else:
        report["primary_key_null_count"] = "N/A"
        report["duplicate_pk_rows"] = "N/A"

    # --- Save Report and Data ---
    try:
        report_df = pd.DataFrame([report])
        report_file = output_dir / f"quality_report_{df_name}.csv"
        report_df.to_csv(report_file, index=False, encoding="utf-8")
        logger.info(f"Quality report saved to: {report_file}")

        df_to_save = (
            validated_df if validation_passed else df
        )  # Save validated if possible
        output_data_file = output_dir / f"{df_name}_processed.csv"
        # Convert boolean explicitly for CSV compatibility
        bool_cols = df_to_save.select_dtypes(
            include=[pd.BooleanDtype(), "boolean"]
        ).columns
        for col in bool_cols:
            df_to_save[col] = df_to_save[col].map(
                {True: True, False: False, pd.NA: None}
            )
        df_to_save.to_csv(
            output_data_file,
            index=False,
            encoding="utf-8",
            date_format="%Y-%m-%d %H:%M:%S",
        )
        logger.info(f"Processed data saved to: {output_data_file}")

        if validation_errors_df is not None and not validation_errors_df.empty:
            error_file = output_dir / f"validation_errors_{df_name}.csv"
            validation_errors_df.to_csv(error_file, index=False, encoding="utf-8")
            logger.info(f"Validation errors saved to: {error_file}")
    except Exception as save_err:
        logger.error(
            f"Failed to save report/data for {df_name}: {save_err}", exc_info=True
        )
        return df, False  # Indicate failure

    logger.info(f"--- Finished Quality Checks for {df_name} ---")
    return df_to_save, True


# --- Normalization Functions (normalize_users, normalize_generic_list, normalize_tags, normalize_employment_types, normalize_reviews - unchanged) ---
def normalize_users(df_flat: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing users...")
    user_cols = [col for col in df_flat.columns if col.startswith("user_")]
    if "user_id" not in user_cols:
        logger.error("Cannot normalize users: 'user_id' missing.")
        return pd.DataFrame()
    users_df = df_flat[user_cols].copy()
    users_df.dropna(subset=["user_id"], inplace=True)
    users_df["user_id"] = users_df["user_id"].astype(pd.Int64Dtype())
    users_df.drop_duplicates(subset=["user_id"], keep="first", inplace=True)
    final_user_cols = [
        "user_id",
        "user_enabled",
        "user_baseType",
        "user_isVerified",
        "user_firstName",
        "user_lastName",
        "user_applicant_hasAEStatus",
        "user_applicant_age",
        "user_grades_count",
        "user_grades_average",
    ]
    users_df = users_df[[col for col in final_user_cols if col in users_df.columns]]
    logger.info(f"Normalized users table: {len(users_df)} unique users.")
    return users_df


def normalize_generic_list(
    df_flat: pd.DataFrame,
    id_col: str,
    json_col: str,
    field_mappings: Dict[str, str],
    new_pk_name: str,
) -> pd.DataFrame:
    logger.info(f"Normalizing generic list: {json_col} linked by {id_col}")
    if id_col not in df_flat.columns or json_col not in df_flat.columns:
        logger.error(f"Missing columns '{id_col}' or '{json_col}'.")
        return pd.DataFrame()
    df_subset = df_flat[[id_col, json_col]].dropna(subset=[json_col, id_col]).copy()
    if df_subset.empty:
        logger.warning(f"No data for {json_col}.")
        return pd.DataFrame()
    df_subset["parsed_json"] = df_subset[json_col].apply(
        lambda x: safe_json_load(x, "list")
    )
    exploded_df = df_subset.explode("parsed_json").dropna(subset=["parsed_json"])
    if exploded_df.empty:
        logger.warning(f"No items after exploding {json_col}.")
        return pd.DataFrame()
    normalized_data = []
    for index, row in exploded_df.iterrows():
        item = row["parsed_json"]
        if isinstance(item, dict):  # Expecting dicts here
            new_row = {id_col: row[id_col]}
            for json_key, col_name in field_mappings.items():
                new_row[col_name] = item.get(json_key)
            normalized_data.append(new_row)
    if not normalized_data:
        logger.warning(f"No dict items found in {json_col}.")
        return pd.DataFrame()
    final_df = pd.DataFrame(normalized_data)
    final_df.reset_index(drop=True, inplace=True)
    final_df[new_pk_name] = final_df.index + 1
    cols_order = [new_pk_name, id_col] + list(field_mappings.values())
    final_df = final_df[[col for col in cols_order if col in final_df.columns]]
    logger.info(f"Normalized {json_col} table: {len(final_df)} rows.")
    return final_df.convert_dtypes()


# FIX 2: Dedicated function for user roles (list of strings)
def normalize_user_roles(df_flat: pd.DataFrame) -> pd.DataFrame:
    """Normalizes the 'user_roles_json' column (list of strings)."""
    logger.info("Normalizing user_roles_json (list of strings)...")
    id_col = "user_id"
    json_col = "user_roles_json"
    new_pk_name = "user_role_pk"

    if id_col not in df_flat.columns or json_col not in df_flat.columns:
        logger.error(
            f"Required columns '{id_col}' or '{json_col}' not found for role normalization."
        )
        return pd.DataFrame()

    df_subset = df_flat[[id_col, json_col]].dropna(subset=[json_col, id_col]).copy()
    if df_subset.empty:
        logger.warning(f"No data to normalize for {json_col}.")
        return pd.DataFrame()

    # Parse JSON - expecting list of strings
    df_subset["parsed_roles"] = df_subset[json_col].apply(
        lambda x: safe_json_load(x, "list")
    )

    # Explode the list into separate rows
    exploded_df = df_subset.explode("parsed_roles")
    exploded_df = exploded_df[
        exploded_df["parsed_roles"].notna() & (exploded_df["parsed_roles"] != "")
    ]  # Remove null/empty roles

    if exploded_df.empty:
        logger.warning(f"No valid roles found after parsing and exploding {json_col}.")
        return pd.DataFrame()

    # Create the final DataFrame
    final_df = exploded_df[[id_col, "parsed_roles"]].copy()
    final_df.rename(columns={"parsed_roles": "role_name"}, inplace=True)
    final_df.drop_duplicates(subset=None, keep="first", inplace=True)

    # Add a generated primary key
    final_df.reset_index(drop=True, inplace=True)
    final_df[new_pk_name] = final_df.index + 1

    # Reorder columns
    cols_order = [new_pk_name, id_col, "role_name"]
    final_df = final_df[cols_order]
    
    

    logger.info(f"Created normalized user_roles table with {len(final_df)} rows.")
    return final_df.convert_dtypes()


def normalize_tags(df_flat: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Normalizing tags_json...")
    tags_list = []
    languages_list = []
    pk_counter_tags = 1
    pk_counter_langs = 1
    if "ad_id" not in df_flat.columns or "tags_json" not in df_flat.columns:
        logger.error("'ad_id' or 'tags_json' missing.")
        return pd.DataFrame(), pd.DataFrame()
    df_subset = (
        df_flat[["ad_id", "tags_json"]].dropna(subset=["ad_id", "tags_json"]).copy()
    )
    tag_types = ["primaryService", "canDoService", "extraService"]
    for index, row in df_subset.iterrows():
        ad_id = row["ad_id"]
        tags_dict = safe_json_load(row["tags_json"], "dict")
        if not tags_dict:
            continue
        for tag_type in tag_types:
            vals = tags_dict.get(tag_type)
            if isinstance(vals, list):
                for v in vals:
                    if v:
                        tags_list.append(
                            {
                                "ad_tag_pk": pk_counter_tags,
                                "ad_id": ad_id,
                                "tag_type": tag_type,
                                "tag_value": str(v),
                            }
                        )
                        pk_counter_tags += 1
            elif vals:
                tags_list.append(
                    {
                        "ad_tag_pk": pk_counter_tags,
                        "ad_id": ad_id,
                        "tag_type": tag_type,
                        "tag_value": str(vals),
                    }
                )
                pk_counter_tags += 1
        langs = tags_dict.get("languages")
        if isinstance(langs, list):
            for idx,lang_dict in enumerate(langs):
                if isinstance(lang_dict, dict) and "code" in lang_dict:
                    languages_list.append(
                        {
                            "ad_language_pk": pk_counter_langs,
                            "ad_id": ad_id,
                            "language_code": lang_dict["code"],
                            "language_index": idx + 1,  # Optional index for ordering
                        }
                    )
                    pk_counter_langs += 1
    tags_df = pd.DataFrame(tags_list)
    languages_df = pd.DataFrame(languages_list)
    logger.info(
        f"Normalized ad_tags: {len(tags_df)} rows. ad_languages: {len(languages_df)} rows."
    )
    return tags_df.convert_dtypes(), languages_df.convert_dtypes()


def normalize_employment_types(df_flat: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing employmentTypes_json into AdRates...")
    rates_list = []
    pk_counter = 1
    if "ad_id" not in df_flat.columns or "employmentTypes_json" not in df_flat.columns:
        logger.error("'ad_id' or 'employmentTypes_json' missing.")
        return pd.DataFrame()
    df_subset = (
        df_flat[["ad_id", "employmentTypes_json"]]
        .dropna(subset=["ad_id", "employmentTypes_json"])
        .copy()
    )
    for index, row in df_subset.iterrows():
        ad_id = row["ad_id"]
        employment_types = safe_json_load(row["employmentTypes_json"], "list")
        if not employment_types:
            continue
        for emp_type in employment_types:
            if not isinstance(emp_type, dict):
                continue
            emp_id = emp_type.get("id")
            care_types = emp_type.get("careTypes")
            if not isinstance(care_types, list):
                continue
            for care_type in care_types:
                if not isinstance(care_type, dict):
                    continue
                care_id = care_type.get("id")
                rate_types = care_type.get("rateTypes")
                if not isinstance(rate_types, list):
                    continue
                for rate_type in rate_types:
                    if not isinstance(rate_type, dict):
                        continue
                    rate_id = rate_type.get("id")
                    rate_details = rate_type.get("rate")
                    if isinstance(rate_details, dict):
                        rates_list.append(
                            {
                                "ad_rate_pk": pk_counter,
                                "ad_id": ad_id,
                                "employment_type": emp_id,
                                "care_type": care_id,
                                "rate_type": rate_id,
                                "rate_amount": pd.to_numeric(
                                    rate_details.get("amount"), errors="coerce"
                                ),
                                "rate_timeUnit": rate_details.get("timeUnit"),
                                "rate_commission": pd.to_numeric(
                                    rate_details.get("commission"), errors="coerce"
                                ),
                            }
                        )
                        pk_counter += 1
    rates_df = pd.DataFrame(rates_list)
    logger.info(f"Normalized ad_rates table: {len(rates_df)} rows.")
    return rates_df.convert_dtypes()


def normalize_reviews(df_flat: pd.DataFrame) -> pd.DataFrame:
    logger.info("Normalizing reviews from user_grades_json...")
    reviews_list = []
    pk_counter = 1
    req_cols = ["ad_id", "user_id", "user_grades_json"]
    if not all(c in df_flat.columns for c in req_cols):
        logger.error(f"Missing required columns for review normalization: {req_cols}")
        return pd.DataFrame()
    df_subset = df_flat[req_cols].dropna(subset=req_cols).copy()
    for index, row in df_subset.iterrows():
        ad_id = row["ad_id"]
        user_id = row["user_id"]
        grades_data = safe_json_load(row["user_grades_json"], "dict")
        if not grades_data:
            continue
        grade_edges = grades_data.get("edges")
        if not isinstance(grade_edges, list):
            continue
        for edge in grade_edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if not isinstance(node, dict):
                continue
            owner = node.get("owner", {})
            reviewer_first = (
                owner.get("firstName", "***") if isinstance(owner, dict) else "***"
            )
            reviewer_last = (
                owner.get("lastName", "*") if isinstance(owner, dict) else "*"
            )
            reviews_list.append(
                {
                    "review_pk": pk_counter,
                    "ad_id": ad_id,
                    "reviewed_user_id": user_id,
                    "reviewer_firstName": reviewer_first,
                    "reviewer_lastName": reviewer_last,
                    "review_content": node.get("content"),
                    "review_generalGrade": pd.to_numeric(
                        node.get("generalGrade"), errors="coerce"
                    ),
                    "review_experienceName": node.get("experienceName"),
                }
            )
            pk_counter += 1
    reviews_df = pd.DataFrame(reviews_list)
    logger.info(f"Normalized reviews table: {len(reviews_df)} rows.")
    return reviews_df.convert_dtypes()


# --- Main Execution ---
if __name__ == "__main__":
    logger.info(
        f"--- Starting Yoopies Data Aggregation, QC, and Normalization (v2) ---"
    )
    logger.info(f"Input base: {BASE_RAW_DIR}, Output base: {BASE_PREPROCESSED_DIR}")

    # 1. Load All Data
    all_page_items = load_all_page_files(BASE_RAW_DIR)
    if not all_page_items:
        logger.error("No page data loaded. Exiting.")
        sys.exit(1)

    # 2. Aggregate Flattened Data
    ads_flat_df = create_aggregated_dataframe(all_page_items)
    if ads_flat_df.empty:
        logger.error("Aggregated flattened DataFrame empty. Exiting.")
        sys.exit(1)

    # 3. Deduplicate Flattened Ads
    if "ad_id" in ads_flat_df.columns:
        initial_count = len(ads_flat_df)
        ads_flat_df.dropna(subset=["ad_id"], inplace=True)
        ads_flat_df["ad_id"] = ads_flat_df["ad_id"].astype(pd.Int64Dtype())
        logger.info(f"Dropped {initial_count - len(ads_flat_df)} ads missing ad_id.")
        count_before_dedupe = len(ads_flat_df)
        ads_flat_df.drop_duplicates(subset=["ad_id"], keep="first", inplace=True)
        logger.info(f"Removed {count_before_dedupe - len(ads_flat_df)} duplicate ads.")
        logger.info(f"Ad count after dedupe: {len(ads_flat_df)}")
    else:
        logger.error("'ad_id' missing. Cannot deduplicate ads.")

    # 4. Initial QC on Flattened Data
    logger.info("--- Initial QC on Flattened Data ---")
    ads_flat_validated_df, flat_qc_success = run_quality_checks_and_report(
        ads_flat_df, flattened_ad_schema, "flattened_ads", BASE_PREPROCESSED_DIR
    )
    if (
        not flat_qc_success and not ads_flat_validated_df.empty
    ):  # Check if failed but df not empty
        logger.warning(
            "Initial QC failed, using original flattened data for normalization."
        )
        ads_flat_validated_df = ads_flat_df  # Fallback to original if validation failed
    elif ads_flat_validated_df.empty:
        logger.error(
            "Flattened dataframe empty after QC. Cannot proceed to normalization. Exiting."
        )
        sys.exit(1)

    # 5. Normalization Steps
    logger.info("--- Starting Normalization ---")
    normalized_tables = {}

    normalized_tables["users"] = normalize_users(ads_flat_validated_df)
    normalized_tables["ad_badges"] = normalize_generic_list(
        ads_flat_validated_df,
        "ad_id",
        "badges_json",
        {"name": "badge_name", "additionalData": "badge_additionalData"},
        "ad_badge_pk",
    )
    normalized_tables["ad_categories"] = normalize_generic_list(
        ads_flat_validated_df,
        "ad_id",
        "categories_json",
        {"service": "service", "category": "category"},
        "ad_category_pk",
    )
    normalized_tables["user_roles"] = normalize_user_roles(
        ads_flat_validated_df
    )  # Use dedicated function
    tags_df, languages_df = normalize_tags(ads_flat_validated_df)
    normalized_tables["ad_tags"] = tags_df
    normalized_tables["ad_languages"] = languages_df
    normalized_tables["ad_rates"] = normalize_employment_types(ads_flat_validated_df)
    normalized_tables["reviews"] = normalize_reviews(ads_flat_validated_df)

    logger.info("Creating final 'ads' table...")
    core_ad_cols = [
        col for col in ads_schema.columns if col in ads_flat_validated_df.columns
    ]
    if "ad_id" in core_ad_cols:
        normalized_tables["ads"] = ads_flat_validated_df[core_ad_cols].copy()
        logger.info(f"Created final ads table: {len(normalized_tables['ads'])} rows.")
    else:
        logger.error("Cannot create final 'ads' table, ad_id missing.")
        normalized_tables["ads"] = pd.DataFrame()

    # 6. QC and Export Normalized Tables
    logger.info("--- QC and Export Normalized Tables ---")
    schemas = {
        "ads": ads_schema,
        "users": users_schema,
        "ad_categories": ad_categories_schema,
        "ad_badges": ad_badges_schema,
        "ad_rates": ad_rates_schema,
        "ad_tags": ad_tags_schema,
        "ad_languages": ad_languages_schema,
        "user_roles": user_roles_schema,
        "reviews": reviews_schema,
    }

    for name, df in normalized_tables.items():
        if name in schemas:
            logger.info(f"--- Processing normalized table: {name} ---")
            # Ensure PK columns are appropriate type before validation if they exist
            pk_col = next((c for c in df.columns if c.endswith("_pk")), None)
            if pk_col and pk_col in df.columns:
                df[pk_col] = pd.to_numeric(df[pk_col], errors="coerce").astype(
                    pd.Int64Dtype()
                )

            validated_df, success = run_quality_checks_and_report(
                df, schemas[name], name, BASE_PREPROCESSED_DIR
            )
            # FIX 4: Log WARNING for empty tables instead of error
            if not success:
                if df.empty:
                    logger.warning(f"Normalized table '{name}' was empty.")
                else:
                    logger.error(
                        f"Quality check or saving failed for normalized table: {name}"
                    )
            else:
                normalized_tables[name] = validated_df  # Keep validated df
        else:
            logger.warning(
                f"No schema defined for normalized table: {name}. Saving without validation."
            )
            try:
                if not df.empty:
                    output_file = BASE_PREPROCESSED_DIR / f"{name}_processed.csv"
                    df.to_csv(
                        output_file,
                        index=False,
                        encoding="utf-8",
                        date_format="%Y-%m-%d %H:%M:%S",
                    )
                    logger.info(
                        f"Saved unvalidated normalized table '{name}' to: {output_file}"
                    )
                else:
                    logger.info(f"Skipping save for empty unvalidated table: {name}")
            except Exception as e:
                logger.error(
                    f"Failed to save unvalidated table '{name}': {e}", exc_info=True
                )

    # 7. Print Final Data Model (Optional)
    # print_proposed_data_model()

    logger.info(f"--- Yoopies Pre-ETL Processing Finished (v2) ---")


def run_data_quality_checks():
    logger.info(
        f"--- Starting Yoopies Data Aggregation, QC, and Normalization (v2) ---"
    )
    logger.info(f"Input base: {BASE_RAW_DIR}, Output base: {BASE_PREPROCESSED_DIR}")

    # 1. Load All Data
    all_page_items = load_all_page_files(BASE_RAW_DIR)
    if not all_page_items:
        logger.error("No page data loaded. Exiting.")
        sys.exit(1)

    # 2. Aggregate Flattened Data
    ads_flat_df = create_aggregated_dataframe(all_page_items)
    if ads_flat_df.empty:
        logger.error("Aggregated flattened DataFrame empty. Exiting.")
        sys.exit(1)

    # 3. Deduplicate Flattened Ads
    if "ad_id" in ads_flat_df.columns:
        initial_count = len(ads_flat_df)
        ads_flat_df.dropna(subset=["ad_id"], inplace=True)
        ads_flat_df["ad_id"] = ads_flat_df["ad_id"].astype(pd.Int64Dtype())
        logger.info(f"Dropped {initial_count - len(ads_flat_df)} ads missing ad_id.")
        count_before_dedupe = len(ads_flat_df)
        ads_flat_df.drop_duplicates(subset=["ad_id"], keep="first", inplace=True)
        logger.info(f"Removed {count_before_dedupe - len(ads_flat_df)} duplicate ads.")
        logger.info(f"Ad count after dedupe: {len(ads_flat_df)}")
    else:
        logger.error("'ad_id' missing. Cannot deduplicate ads.")

    # 4. Initial QC on Flattened Data
    logger.info("--- Initial QC on Flattened Data ---")
    ads_flat_validated_df, flat_qc_success = run_quality_checks_and_report(
        ads_flat_df, flattened_ad_schema, "flattened_ads", BASE_PREPROCESSED_DIR
    )
    if (
        not flat_qc_success and not ads_flat_validated_df.empty
    ):  # Check if failed but df not empty
        logger.warning(
            "Initial QC failed, using original flattened data for normalization."
        )
        ads_flat_validated_df = ads_flat_df  # Fallback to original if validation failed
    elif ads_flat_validated_df.empty:
        logger.error(
            "Flattened dataframe empty after QC. Cannot proceed to normalization. Exiting."
        )
        sys.exit(1)

    # 5. Normalization Steps
    logger.info("--- Starting Normalization ---")
    normalized_tables = {}

    normalized_tables["users"] = normalize_users(ads_flat_validated_df)
    normalized_tables["ad_badges"] = normalize_generic_list(
        ads_flat_validated_df,
        "ad_id",
        "badges_json",
        {"name": "badge_name", "additionalData": "badge_additionalData"},
        "ad_badge_pk",
    )
    normalized_tables["ad_categories"] = normalize_generic_list(
        ads_flat_validated_df,
        "ad_id",
        "categories_json",
        {"service": "service", "category": "category"},
        "ad_category_pk",
    )
    normalized_tables["user_roles"] = normalize_user_roles(
        ads_flat_validated_df
    )  # Use dedicated function
    tags_df, languages_df = normalize_tags(ads_flat_validated_df)
    normalized_tables["ad_tags"] = tags_df
    normalized_tables["ad_languages"] = languages_df
    normalized_tables["ad_rates"] = normalize_employment_types(ads_flat_validated_df)
    normalized_tables["reviews"] = normalize_reviews(ads_flat_validated_df)

    logger.info("Creating final 'ads' table...")
    core_ad_cols = [
        col for col in ads_schema.columns if col in ads_flat_validated_df.columns
    ]
    if "ad_id" in core_ad_cols:
        normalized_tables["ads"] = ads_flat_validated_df[core_ad_cols].copy()
        logger.info(f"Created final ads table: {len(normalized_tables['ads'])} rows.")
    else:
        logger.error("Cannot create final 'ads' table, ad_id missing.")
        normalized_tables["ads"] = pd.DataFrame()

    # 6. QC and Export Normalized Tables
    logger.info("--- QC and Export Normalized Tables ---")
    schemas = {
        "ads": ads_schema,
        "users": users_schema,
        "ad_categories": ad_categories_schema,
        "ad_badges": ad_badges_schema,
        "ad_rates": ad_rates_schema,
        "ad_tags": ad_tags_schema,
        "ad_languages": ad_languages_schema,
        "user_roles": user_roles_schema,
        "reviews": reviews_schema,
    }

    for name, df in normalized_tables.items():
        if name in schemas:
            logger.info(f"--- Processing normalized table: {name} ---")
            # Ensure PK columns are appropriate type before validation if they exist
            pk_col = next((c for c in df.columns if c.endswith("_pk")), None)
            if pk_col and pk_col in df.columns:
                df[pk_col] = pd.to_numeric(df[pk_col], errors="coerce").astype(
                    pd.Int64Dtype()
                )

            validated_df, success = run_quality_checks_and_report(
                df, schemas[name], name, BASE_PREPROCESSED_DIR
            )
            # FIX 4: Log WARNING for empty tables instead of error
            if not success:
                if df.empty:
                    logger.warning(f"Normalized table '{name}' was empty.")
                else:
                    logger.error(
                        f"Quality check or saving failed for normalized table: {name}"
                    )
            else:
                normalized_tables[name] = validated_df  # Keep validated df
        else:
            logger.warning(
                f"No schema defined for normalized table: {name}. Saving without validation."
            )
            try:
                if not df.empty:
                    output_file = BASE_PREPROCESSED_DIR / f"{name}_processed.csv"
                    df.to_csv(
                        output_file,
                        index=False,
                        encoding="utf-8",
                        date_format="%Y-%m-%d %H:%M:%S",
                    )
                    logger.info(
                        f"Saved unvalidated normalized table '{name}' to: {output_file}"
                    )
                else:
                    logger.info(f"Skipping save for empty unvalidated table: {name}")
            except Exception as e:
                logger.error(
                    f"Failed to save unvalidated table '{name}': {e}", exc_info=True
                )

    # 7. Print Final Data Model (Optional)
    # print_proposed_data_model()

    logger.info(f"--- Yoopies Pre-ETL Processing Finished (v2) ---")
