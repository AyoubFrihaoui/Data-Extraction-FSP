# src/etl/odete_pre_etl_processor_v4.py (Corrected Normalized Schema)

import os
import json
import logging
import traceback
import re
import sys
import time 
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check, Index
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_RAW_DIR = Path("D:/ODETE/ODETES/ODETE/odete.com.br/diarista/") 
BASE_PREPROCESSED_DIR = Path("odete_data/processed/")

# --- Logging Setup ---
LOG_FILE = BASE_PREPROCESSED_DIR / "data_processing_odete_v4.log" # New log file name

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
    format="%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# --- Helper Functions (safe_json_dump, safe_json_load, extract_worker_id_from_filename, parse_html_to_profile_json, load_all_html_files) ---
# These functions remain unchanged from your v3, assuming they are working correctly.
# For brevity, I'll omit them here but they should be included in the full script.

def safe_json_dump(data: Any, default_behavior: str = "null") -> Optional[str]:
    """Safely dumps Python data to a JSON string."""
    try:
        return json.dumps(data, default=str) 
    except TypeError as e:
        logger.warning(f"TypeError during JSON dump for data snippet '{str(data)[:100]}': {e}. Returning default.")
        if default_behavior == "null":
            return None 
        elif default_behavior == "empty_list":
            return "[]"
        elif default_behavior == "empty_dict":
            return "{}"
        return None

def safe_json_load(json_string: Optional[str], return_type: str = "list") -> Any:
    """Safely loads a JSON string into a Python object."""
    default_return = [] if return_type == "list" else {} if return_type == "dict" else None
    if pd.isna(json_string) or not isinstance(json_string, str):
        return default_return
    try:
        loaded_data = json.loads(json_string)
        if return_type == "list" and not isinstance(loaded_data, list):
            return [loaded_data] if loaded_data is not None else []
        if return_type == "dict" and not isinstance(loaded_data, dict):
            return {} 
        return loaded_data
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"JSONDecodeError or TypeError: '{e}' for JSON string snippet: '{str(json_string)[:100]}'. Returning default.")
        return default_return

def extract_worker_id_from_filename(file_path: Path) -> str:
    """Extracts the worker ID from the filename."""
    return file_path.stem

def parse_html_to_profile_json(html_content: str, filename: str) -> Optional[Dict[str, Any]]:
    """Parses HTML to extract the profile JSON object."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        next_data_script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not next_data_script_tag:
            logger.warning(f"__NEXT_DATA__ script tag not found in file: {filename}")
            return None
        json_data_string = next_data_script_tag.string
        if not json_data_string:
            logger.warning(f"__NEXT_DATA__ script tag is empty in file: {filename}")
            return None
        data_object = json.loads(json_data_string)
        profile_data = data_object.get('props', {}).get('pageProps', {}).get('profile', None)
        if not profile_data:
            logger.warning(f"Profile data (props.pageProps.profile) not found in __NEXT_DATA__ for file: {filename}")
            return None
        return profile_data
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error in file {filename}: {e}. Check HTML structure.")
        return None
    except Exception as e: 
        logger.error(f"Generic error parsing HTML for file {filename}: {e}", exc_info=True)
        return None

def load_all_html_files(base_directory: Path) -> List[Dict[str, Any]]:
    """Loads all HTML files, extracts worker_id and profile JSON."""
    profile_items_list = []
    if not base_directory.is_dir():
        logger.error(f"Base input directory '{base_directory}' not found or is not a directory.")
        return profile_items_list
    logger.info(f"Starting scan for HTML files in directory: {base_directory}")
    all_files_in_dir = list(base_directory.glob("*")) 
    html_files_paths = [f for f in all_files_in_dir if f.is_file()]
    total_files = len(html_files_paths)
    logger.info(f"Found {total_files} potential files to process.")
    processed_count = 0
    for i, file_path in enumerate(html_files_paths):
        if (i + 1) % 100 == 0 : 
             logger.info(f"Processing file {i+1}/{total_files}: {file_path.name}")
        worker_id = extract_worker_id_from_filename(file_path)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                html_content = f.read()
            profile_json_content = parse_html_to_profile_json(html_content, file_path.name)
            if profile_json_content: 
                profile_items_list.append({
                    "worker_id_from_filename": worker_id,
                    "profile_data": profile_json_content,
                    "source_filename": file_path.name    
                })
                processed_count += 1
        except Exception as e:
            logger.error(f"Failed to read/process file {file_path.name}: {e}", exc_info=False)
            logger.debug(f"Detailed error for {file_path.name}:", exc_info=True)
    logger.info(f"Successfully loaded and parsed profile data from {processed_count} out of {total_files} files.")
    return profile_items_list

# --- Flattening Function (Unchanged from v3) ---
def flatten_odete_profile(profile_item: Dict[str, Any]) -> Dict[str, Any]:
    """Flattens the nested profile JSON data."""
    flat_data = {"_flattening_error": False}
    profile_data_json = profile_item.get("profile_data", {})
    flat_data["worker_id_from_filename"] = profile_item.get("worker_id_from_filename")
    flat_data["source_filename"] = profile_item.get("source_filename")
    try:
        core_fields = [
            "_id", "enabledInstagramFeed", "fullSchedule", "online", "next", 
            "nome", "sobrenome", "historico", "uf", "imagem", "cidade", 
            "bairro", "valor", "ultimoPagamento", "ultimoAcesso", "createdAt",
            "totalDicas", "totalRecomendacoes", "totalMatchs", "totalSelos",
            "totalPropostasVisualizadas", "totalScore", "porcentagemContratacao",
            "tempoMedioVisualizacaoOferta", "rating"
        ]
        for field in core_fields:
            flat_data[f"profile_{field}"] = profile_data_json.get(field)

        list_object_fields_to_json_dump = {
            "servicos": "profile_servicos_json", "regioes": "profile_regioes_json",
            "comentarios": "profile_comentarios_json", "solicitacoes": "profile_solicitacoes_json",
            "selos": "profile_selos_json", "hints": "profile_hints_json",
            "mesesPagos": "profile_mesesPagos_json"
        }
        for raw_field_name, new_flat_field_name in list_object_fields_to_json_dump.items():
            raw_value = profile_data_json.get(raw_field_name)
            flat_data[new_flat_field_name] = safe_json_dump(raw_value if raw_value is not None else [])

        datetime_fields = ["profile_ultimoPagamento", "profile_ultimoAcesso", "profile_createdAt"]
        for dt_field in datetime_fields:
            if flat_data.get(dt_field):
                flat_data[dt_field] = pd.to_datetime(flat_data[dt_field], errors='coerce', utc=True)
        
        boolean_fields = ["profile_enabledInstagramFeed", "profile_fullSchedule", "profile_online"]
        for bool_field in boolean_fields:
            val = flat_data.get(bool_field)
            flat_data[bool_field] = bool(val) if val is not None else None

        numeric_fields = [
            "profile_next", "profile_valor", "profile_totalDicas", "profile_totalRecomendacoes",
            "profile_totalMatchs", "profile_totalSelos", "profile_totalPropostasVisualizadas",
            "profile_totalScore", "profile_porcentagemContratacao", "profile_tempoMedioVisualizacaoOferta",
            "profile_rating"
        ]
        for num_field in numeric_fields:
            if flat_data.get(num_field) is not None:
                flat_data[num_field] = pd.to_numeric(flat_data[num_field], errors='coerce')
        
        flat_data["profile_id"] = flat_data.pop("profile__id", None)
        if flat_data["profile_id"] is None:
             logger.warning(f"Critical: Profile ID ('_id') is missing in profile data for file {flat_data.get('source_filename')}.")
             flat_data["_flattening_error"] = True
    except Exception as e:
        logger.error(f"Error during flattening of profile_id {flat_data.get('profile_id', 'N/A')} "
                     f"(source_filename: {flat_data.get('source_filename', 'N/A')}): {e}", exc_info=True)
        flat_data["_flattening_error"] = True
    return flat_data

# --- DataFrame Creation (Unchanged from v3) ---
def create_aggregated_dataframe(loaded_profile_items: List[Dict[str, Any]]) -> pd.DataFrame:
    """Creates a single aggregated (flattened) Pandas DataFrame."""
    logger.info(f"Starting aggregation of {len(loaded_profile_items)} loaded profile items...")
    flattened_profile_records = []
    items_with_errors = 0
    for i, item in enumerate(loaded_profile_items):
        if (i + 1) % 500 == 0: 
            logger.info(f"Flattening item {i+1}/{len(loaded_profile_items)}...")
        if not isinstance(item.get("profile_data"), dict):
            logger.warning(f"Skipping item from '{item.get('source_filename')}' due to missing or invalid 'profile_data'.")
            items_with_errors += 1
            continue
        flat_profile_record = flatten_odete_profile(item)
        if not flat_profile_record.get("_flattening_error"):
            flattened_profile_records.append(flat_profile_record)
        else:
            logger.warning(f"Skipping profile from '{item.get('source_filename')}' due to an error during flattening.")
            items_with_errors +=1
    if items_with_errors > 0:
        logger.warning(f"Encountered errors while flattening {items_with_errors} items.")
    if not flattened_profile_records:
        logger.error("No valid profile data was successfully flattened. Resulting DataFrame will be empty.")
        return pd.DataFrame()
    aggregated_df = pd.DataFrame(flattened_profile_records)
    if "_flattening_error" in aggregated_df.columns:
        aggregated_df = aggregated_df[aggregated_df["_flattening_error"] == False].drop(columns=["_flattening_error"])
    if "profile_id" in aggregated_df.columns:
        initial_row_count = len(aggregated_df)
        aggregated_df.dropna(subset=["profile_id"], inplace=True)
        if len(aggregated_df) < initial_row_count:
            logger.info(f"Dropped {initial_row_count - len(aggregated_df)} rows with missing 'profile_id'.")
        current_row_count = len(aggregated_df)
        aggregated_df.drop_duplicates(subset=["profile_id"], keep="first", inplace=True)
        num_duplicates_removed = current_row_count - len(aggregated_df)
        if num_duplicates_removed > 0:
            logger.info(f"Removed {num_duplicates_removed} duplicate profiles based on 'profile_id'.")
    else:
        logger.error("Critical: Column 'profile_id' not found in DataFrame. Deduplication cannot be performed effectively.")
        if "worker_id_from_filename" in aggregated_df.columns:
            logger.warning("Attempting deduplication using 'worker_id_from_filename' as a fallback for 'profile_id'.")
            current_row_count = len(aggregated_df)
            aggregated_df.drop_duplicates(subset=["worker_id_from_filename"], keep="first", inplace=True)
            num_duplicates_removed = current_row_count - len(aggregated_df)
            if num_duplicates_removed > 0:
                 logger.info(f"Removed {num_duplicates_removed} duplicate profiles based on 'worker_id_from_filename'.")
    try:
        aggregated_df = aggregated_df.convert_dtypes()
    except Exception as e:
        logger.error(f"Error during DataFrame.convert_dtypes(): {e}. Data types might be suboptimal.", exc_info=True)
    logger.info(f"Successfully created aggregated DataFrame with {len(aggregated_df)} unique profiles and {len(aggregated_df.columns)} columns.")
    return aggregated_df


# --- Pandera Schemas ---
# Flattened profile schema (corrected in v3, unchanged here)
flattened_profile_schema = DataFrameSchema(
    columns={
        "profile_id": Column(str, nullable=False, unique=True, required=True, title="Profile Primary ID"),
        "worker_id_from_filename": Column(str, nullable=False, description="Worker ID derived from the source filename"),
        "source_filename": Column(str, nullable=False, description="Original HTML filename"),
        "profile_nome": Column(str, nullable=True, coerce=True, description="Worker's first name"),
        "profile_sobrenome": Column(str, nullable=True, coerce=True, description="Worker's last name"),
        "profile_historico": Column(str, nullable=True, coerce=True, description="Worker's biography/history text"),
        "profile_uf": Column(str, nullable=True, coerce=True, description="State (Unidade Federativa)"),
        "profile_cidade": Column(str, nullable=True, coerce=True, description="City"),
        "profile_bairro": Column(str, nullable=True, coerce=True, description="Neighborhood"),
        "profile_valor": Column(float, nullable=True, coerce=True, checks=Check.ge(0), description="Price/value of service"),
        "profile_createdAt": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Profile creation timestamp"),
        "profile_ultimoAcesso": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Last access timestamp"),
        "profile_ultimoPagamento": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Last payment timestamp"),
        "profile_rating": Column(float, nullable=True, coerce=True, checks=Check.in_range(0,5, include_min=True, include_max=True), description="Overall profile rating (0-5)"),
        "profile_online": Column(pd.BooleanDtype(), nullable=True, coerce=True, description="Online status"),
        "profile_servicos_json": Column(str, nullable=True, coerce=True, description="JSON string of services offered"),
        "profile_regioes_json": Column(str, nullable=True, coerce=True, description="JSON string of regions served"),
        "profile_comentarios_json": Column(str, nullable=True, coerce=True, description="JSON string of comments/recommendations"),
        "profile_solicitacoes_json": Column(str, nullable=True, coerce=True, description="JSON string of job solicitations/matches"),
        "profile_selos_json": Column(str, nullable=True, coerce=True, description="JSON string of badges/selos earned"),
        "profile_hints_json": Column(str, nullable=True, coerce=True, description="JSON string of hints"),
        "profile_mesesPagos_json": Column(str, nullable=True, coerce=True, description="JSON string of payment history by month"),
        "profile_totalRecomendacoes": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)),
        "profile_totalMatchs": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)),
        "profile_totalSelos": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)),
        "profile_totalScore": Column(pd.Int64Dtype(), nullable=True, coerce=True),
    },
    strict=False, 
    coerce=True,
    unique=["profile_id"]
)

# --- Quality Check Function (Unchanged from v3) ---
def run_quality_checks_and_report(
    df: pd.DataFrame, schema: DataFrameSchema, df_name: str, output_dir: Path
) -> Tuple[pd.DataFrame, bool]:
    """Runs data quality checks and saves reports."""
    logger.info(f"--- Running Quality Checks for DataFrame: '{df_name}' (Rows: {len(df)}) ---")
    start_time_qc = time.time()
    if df.empty:
        logger.warning(f"DataFrame '{df_name}' is empty. Skipping validation.")
        report_data = {"dataframe_name": df_name, "total_rows": 0, "status": "empty", "schema_validation_passed": "N/A"}
        report_df = pd.DataFrame([report_data])
        report_file = output_dir / f"quality_report_{df_name}.csv"
        try:
            report_df.to_csv(report_file, index=False, encoding="utf-8")
            logger.info(f"Saved empty data report to: {report_file}")
        except Exception as e:
            logger.error(f"Failed to save empty data report for '{df_name}': {e}", exc_info=True)
        return df, False
    validated_df = df.copy()
    validation_errors_df = None
    validation_passed = False
    validation_error_count = 0
    df_for_stats = df.copy() 
    try:
        if isinstance(validated_df.index, pd.MultiIndex) and schema.index is not None and schema.index.unique:
             logger.warning(f"DataFrame '{df_name}' has MultiIndex, resetting index.")
             validated_df.reset_index(inplace=True)
        validated_df = schema.validate(validated_df, lazy=True)
        logger.info(f"Schema validation PASSED for '{df_name}'.")
        validation_passed = True
        df_for_stats = validated_df.copy()
    except pa.errors.SchemaErrors as err:
        logger.error(f"Schema validation FAILED for '{df_name}'. Number of errors: {len(err.failure_cases)}")
        validation_errors_df = err.failure_cases
        validation_error_count = len(validation_errors_df)
        try:
            error_sample_dict = validation_errors_df.head().to_dict(orient="records")
            logger.error(f"Validation Errors Sample for '{df_name}' (first 5):\n{json.dumps(error_sample_dict, indent=2, default=str)}")
        except Exception:
            logger.error("Could not display a sample of validation errors for '{df_name}'.")
    except Exception as verr:
        logger.error(f"Unexpected error during Pandera validation for '{df_name}': {verr}", exc_info=True)
        validation_error_count = -1
    quality_report_data = {
        "dataframe_name": df_name, "total_rows": len(df_for_stats),
        "total_columns": len(df_for_stats.columns), "schema_validation_passed": validation_passed,
        "validation_error_count": validation_error_count,
    }
    missing_values_counts = df_for_stats.isnull().sum()
    quality_report_data["missing_values_total"] = int(missing_values_counts.sum())
    quality_report_data["missing_values_per_column_json"] = safe_json_dump(
        missing_values_counts[missing_values_counts > 0].astype(int).to_dict()
    )
    dtype_summary_dict = {col: str(df_for_stats[col].dtype) for col in df_for_stats.columns}
    quality_report_data["data_types_per_column_json"] = safe_json_dump(dtype_summary_dict)
    pk_column_name = None
    if schema.index and schema.index.unique and schema.index.name:
        pk_column_name = schema.index.name
    elif schema.columns:
        unique_cols_in_schema = [col_name for col_name, col_obj in schema.columns.items() if col_obj.unique]
        if unique_cols_in_schema:
            pk_column_name = unique_cols_in_schema[0]
            if len(unique_cols_in_schema) > 1:
                logger.info(f"Multiple unique columns in schema for '{df_name}'. Using '{pk_column_name}' for PK checks.")
    if pk_column_name and pk_column_name in df_for_stats.columns:
        null_pk_count = df_for_stats[pk_column_name].isnull().sum()
        quality_report_data["primary_key_null_count"] = int(null_pk_count)
        num_pk_duplicates = int(df_for_stats[pk_column_name].dropna().duplicated().sum())
        quality_report_data["duplicate_pk_values_count"] = num_pk_duplicates
        if null_pk_count > 0: logger.warning(f"{null_pk_count} null PKs ('{pk_column_name}') in '{df_name}'.")
        if num_pk_duplicates > 0: logger.warning(f"{num_pk_duplicates} duplicate PKs ('{pk_column_name}') in '{df_name}'.")
    else:
        quality_report_data["primary_key_null_count"] = "N/A"
        quality_report_data["duplicate_pk_values_count"] = "N/A"
        logger.info(f"No single PK identified for detailed checks in '{df_name}'.")
    try:
        report_df = pd.DataFrame([quality_report_data])
        report_file_path = output_dir / f"quality_report_{df_name}.csv"
        report_df.to_csv(report_file_path, index=False, encoding="utf-8")
        logger.info(f"Quality report for '{df_name}' saved to: {report_file_path}")
        df_to_be_saved = validated_df if validation_passed else df
        output_data_file_path = output_dir / f"{df_name}_processed.csv"
        bool_cols_to_convert = df_to_be_saved.select_dtypes(include=[pd.BooleanDtype(), "boolean"]).columns
        for col in bool_cols_to_convert:
            df_to_be_saved[col] = df_to_be_saved[col].map({True: True, False: False, pd.NA: None})
        df_to_be_saved.to_csv(output_data_file_path, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
        logger.info(f"Processed data for '{df_name}' saved to: {output_data_file_path}")
        if validation_errors_df is not None and not validation_errors_df.empty:
            error_file_path = output_dir / f"validation_errors_{df_name}.csv"
            validation_errors_df.to_csv(error_file_path, index=False, encoding="utf-8")
            logger.info(f"Validation error details for '{df_name}' saved to: {error_file_path}")
    except Exception as save_err:
        logger.error(f"Failed to save report/data for '{df_name}': {save_err}", exc_info=True)
        return df, False
    qc_duration = time.time() - start_time_qc
    logger.info(f"--- Finished Quality Checks for '{df_name}' in {qc_duration:.2f} seconds ---")
    return df_to_be_saved, True

# --- Normalization Functions (Unchanged from v3) ---
def normalize_profiles_table(df_flat: pd.DataFrame, schema_cols: list) -> pd.DataFrame:
    """Creates the main 'Profiles' table."""
    logger.info("Normalizing 'Profiles' table...")
    relevant_profile_cols = [col for col in schema_cols if col in df_flat.columns]
    if "profile_id" not in relevant_profile_cols:
        logger.error("'profile_id' missing. Cannot create Profiles table.")
        return pd.DataFrame()
    profiles_df = df_flat[relevant_profile_cols].copy()
    try:
        profiles_df.set_index("profile_id", inplace=True, verify_integrity=True)
    except KeyError:
        logger.error("'profile_id' not found to set as index.")
        return pd.DataFrame()
    except Exception as e: 
        logger.error(f"Error setting 'profile_id' as index: {e}. Dropping duplicates and retrying.")
        profiles_df.drop_duplicates(subset=["profile_id"], inplace=True)
        profiles_df.set_index("profile_id", inplace=True)
    logger.info(f"Created 'Profiles' table with {len(profiles_df)} rows.")
    return profiles_df

def normalize_services(df_flat: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes services data."""
    logger.info("Normalizing services...")
    all_profile_services = []
    unique_services = set()
    if "profile_id" not in df_flat.columns or "profile_servicos_json" not in df_flat.columns:
        logger.error("Missing 'profile_id' or 'profile_servicos_json' for service normalization.")
        return pd.DataFrame(), pd.DataFrame()
    for index, row in df_flat.iterrows():
        profile_id = row["profile_id"]
        services_list = safe_json_load(row["profile_servicos_json"], "list")
        if services_list:
            for service_name in services_list:
                if isinstance(service_name, str) and service_name.strip():
                    service_name_clean = service_name.strip()
                    unique_services.add(service_name_clean)
                    all_profile_services.append({"profile_id": profile_id, "service_name": service_name_clean})
    if not unique_services:
        logger.info("No unique services found.")
        return pd.DataFrame(), pd.DataFrame(all_profile_services) if all_profile_services else pd.DataFrame()
    services_master_df = pd.DataFrame(list(unique_services), columns=["service_name"])
    services_master_df.sort_values("service_name", inplace=True)
    services_master_df.reset_index(drop=True, inplace=True)
    services_master_df["service_id"] = services_master_df.index + 1
    services_master_df = services_master_df[["service_id", "service_name"]]
    logger.info(f"Created 'services_master' table: {len(services_master_df)} services.")
    if not all_profile_services:
        logger.info("No profile-service relationships for junction table.")
        return services_master_df, pd.DataFrame()
    profile_services_df = pd.DataFrame(all_profile_services)
    profile_services_junction_df = pd.merge(profile_services_df, services_master_df, on="service_name", how="left")
    profile_services_junction_df = profile_services_junction_df[["profile_id", "service_id"]]
    profile_services_junction_df.dropna(subset=["service_id"], inplace=True)
    profile_services_junction_df["service_id"] = profile_services_junction_df["service_id"].astype(int)
    profile_services_junction_df.reset_index(drop=True, inplace=True)
    profile_services_junction_df["profile_service_pk"] = profile_services_junction_df.index + 1
    profile_services_junction_df = profile_services_junction_df[["profile_service_pk", "profile_id", "service_id"]]
    logger.info(f"Created 'profile_services_junction' table: {len(profile_services_junction_df)} links.")
    return services_master_df, profile_services_junction_df

# --- Main Execution Block ---
def run_data_processing():
    """Main function to orchestrate the ETL process."""
    overall_start_time = time.time()
    logger.info(f"--- Starting Odete.com.br Data Processing (v4) ---")
    logger.info(f"Input base directory: {BASE_RAW_DIR}")
    logger.info(f"Output base directory: {BASE_PREPROCESSED_DIR}")

    start_time_load = time.time()
    all_profile_items = load_all_html_files(BASE_RAW_DIR)
    logger.info(f"HTML loading and initial parsing took {time.time() - start_time_load:.2f} seconds.")
    if not all_profile_items:
        logger.error("No profile data loaded. Terminating.")
        sys.exit(1)

    start_time_flatten_agg = time.time()
    profiles_flat_df = create_aggregated_dataframe(all_profile_items)
    logger.info(f"Flattening and aggregation took {time.time() - start_time_flatten_agg:.2f} seconds.")
    if profiles_flat_df.empty:
        logger.error("Aggregated DataFrame empty. Terminating.")
        sys.exit(1)
    
    logger.info("--- Initial QC on Flattened Data ---")
    profiles_flat_validated_df, flat_qc_passed = run_quality_checks_and_report(
        profiles_flat_df, flattened_profile_schema, "flattened_profiles", BASE_PREPROCESSED_DIR
    )
    if not flat_qc_passed and profiles_flat_validated_df.empty:
        logger.error("Initial QC failed AND DataFrame is empty. Terminating.")
        sys.exit(1)
    elif not flat_qc_passed:
        logger.warning("Initial QC failed. Proceeding with unvalidated data for normalization.")
    else:
        logger.info("Initial QC passed for flattened_profiles.")
    
    logger.info("--- Starting Normalization ---")
    start_time_norm = time.time()
    normalized_tables = {}
    final_profile_table_columns = [ # Columns for the main 'profiles' table after normalization
        "profile_id", "worker_id_from_filename", "source_filename", "profile_enabledInstagramFeed",
        "profile_fullSchedule", "profile_online", "profile_next", "profile_nome", "profile_sobrenome",
        "profile_historico", "profile_uf", "profile_imagem", "profile_cidade", "profile_bairro",
        "profile_valor", "profile_ultimoPagamento", "profile_ultimoAcesso", "profile_createdAt",
        "profile_totalDicas", "profile_totalRecomendacoes", "profile_totalMatchs", "profile_totalSelos",
        "profile_totalPropostasVisualizadas", "profile_totalScore", "profile_porcentagemContratacao",
        "profile_tempoMedioVisualizacaoOferta", "profile_rating"
    ]
    normalized_tables["profiles"] = normalize_profiles_table(profiles_flat_validated_df, final_profile_table_columns)
    
    services_master_df, profile_services_junction_df = normalize_services(profiles_flat_validated_df)
    normalized_tables["services_master"] = services_master_df
    normalized_tables["profile_services_junction"] = profile_services_junction_df
    
    logger.info(f"Normalization phase took {time.time() - start_time_norm:.2f} seconds.")

    logger.info("--- QC and Export Normalized Tables ---")
    
    # CORRECTED profiles_normalized_schema
    profiles_normalized_schema = DataFrameSchema(
        columns={ 
            "worker_id_from_filename": Column(str, nullable=False), 
            "profile_nome": Column(str, nullable=True, coerce=True),
            "profile_sobrenome": Column(str, nullable=True, coerce=True), 
            "profile_uf": Column(str, nullable=True, coerce=True),
            "profile_cidade": Column(str, nullable=True, coerce=True),
            "profile_valor": Column(float, nullable=True, coerce=True, checks=Check.ge(0)), # Corrected
            "profile_createdAt": Column("datetime64[ns, UTC]", nullable=True, coerce=True),
            # Add all other columns from final_profile_table_columns that are NOT the index
             "source_filename": Column(str, nullable=False),
             "profile_enabledInstagramFeed": Column(pd.BooleanDtype(), nullable=True),
             "profile_fullSchedule": Column(pd.BooleanDtype(), nullable=True),
             "profile_online": Column(pd.BooleanDtype(), nullable=True),
             "profile_next": Column(pd.Int64Dtype(), nullable=True), # Assuming 'next' is integer-like
             "profile_historico": Column(str, nullable=True),
             "profile_imagem": Column(str, nullable=True), # Filename/URL part
             "profile_bairro": Column(str, nullable=True),
             "profile_ultimoPagamento": Column("datetime64[ns, UTC]", nullable=True),
             "profile_ultimoAcesso": Column("datetime64[ns, UTC]", nullable=True),
             "profile_totalDicas": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_totalRecomendacoes": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_totalMatchs": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_totalSelos": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_totalPropostasVisualizadas": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_totalScore": Column(pd.Int64Dtype(), nullable=True),
             "profile_porcentagemContratacao": Column(float, nullable=True, checks=Check.ge(0)), # or Int if always whole numbers
             "profile_tempoMedioVisualizacaoOferta": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)),
             "profile_rating": Column(float, nullable=True, checks=Check.in_range(0,5)),
        }, 
        index=Index(str, name="profile_id", unique=True, nullable=False), 
        strict=False, # To allow columns not explicitly defined if they exist from schema_cols
        coerce=True
    )

    services_master_schema = DataFrameSchema({
        "service_id": Column(int, unique=True, nullable=False), 
        "service_name": Column(str, nullable=False)
    }, strict=True, coerce=True)

    profile_services_junction_schema = DataFrameSchema({
        "profile_service_pk": Column(int, unique=True, nullable=False),
        "profile_id": Column(str, nullable=False), 
        "service_id": Column(int, nullable=False)
    }, strict=True, coerce=True)

    schemas_for_normalized_tables = {
         "profiles": profiles_normalized_schema, 
         "services_master": services_master_schema,
         "profile_services_junction": profile_services_junction_schema,
    }

    for table_name, norm_df in normalized_tables.items():
        logger.info(f"--- Processing Normalized Table: '{table_name}' ---")
        if norm_df.empty:
            logger.info(f"Normalized table '{table_name}' empty. Creating empty report/file.")
            empty_output_file = BASE_PREPROCESSED_DIR / f"{table_name}_processed.csv"
            pd.DataFrame().to_csv(empty_output_file, index=False)
            # Create a quality report stating it's empty
            run_quality_checks_and_report(pd.DataFrame(), DataFrameSchema(name=f"empty_schema_for_{table_name}"), table_name, BASE_PREPROCESSED_DIR)
            continue
        
        schema_to_use = schemas_for_normalized_tables.get(table_name)
        if schema_to_use:
            df_for_validation = norm_df
            # Pandera validates index if schema.index is defined.
            # If df's index name matches schema's index name, it's fine.
            # If df's index is PK but schema expects it as column, df.reset_index() before validation.
            if schema_to_use.index and schema_to_use.index.name:
                if norm_df.index.name != schema_to_use.index.name:
                    if schema_to_use.index.name in norm_df.columns:
                        logger.info(f"Index name mismatch for '{table_name}'. DF index: '{norm_df.index.name}', Schema index: '{schema_to_use.index.name}'. Will attempt to set DF index from column '{schema_to_use.index.name}'.")
                        try:
                            df_for_validation = norm_df.set_index(schema_to_use.index.name, verify_integrity=True)
                        except Exception as e_set_idx:
                            logger.error(f"Could not set index '{schema_to_use.index.name}' for table '{table_name}' for validation: {e_set_idx}. Validating with current index/columns.")
                            # If setting index fails, proceed with current df_for_validation, Pandera might complain if index doesn't match schema.
                    else: # Schema index name not in columns, maybe reset index?
                         logger.info(f"Schema for '{table_name}' expects index '{schema_to_use.index.name}', which is not in columns. Resetting DF index for validation.")
                         df_for_validation = norm_df.reset_index()

            elif not schema_to_use.index and norm_df.index.name is not None: # Schema has no index, DF has one
                logger.info(f"Schema for '{table_name}' expects no index, but DataFrame has index '{norm_df.index.name}'. Resetting DF index for validation.")
                df_for_validation = norm_df.reset_index()


            validated_df, qc_success = run_quality_checks_and_report(
                df_for_validation, schema_to_use, table_name, BASE_PREPROCESSED_DIR
            )
            if not qc_success: 
                logger.error(f"QC or saving failed for '{table_name}'.")
        else:
            logger.warning(f"No schema for '{table_name}'. Saving without validation.")
            try:
                output_file_path = BASE_PREPROCESSED_DIR / f"{table_name}_processed.csv"
                should_save_index = True if norm_df.index.name and norm_df.index.name != "index" else False # Save meaningful index
                norm_df.to_csv(output_file_path, index=should_save_index, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
                logger.info(f"Saved unvalidated table '{table_name}' to: {output_file_path}")
            except Exception as e:
                logger.error(f"Failed to save unvalidated table '{table_name}': {e}", exc_info=True)
                
    overall_duration = time.time() - overall_start_time
    logger.info(f"--- Odete.com.br Data Processing Finished in {overall_duration:.2f} seconds ---")

if __name__ == "__main__":
    run_data_processing()