# src/etl/odete_pre_etl_processor_v5.py (Normalization Expansion & Translation)

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
LOG_FILE = BASE_PREPROCESSED_DIR / "data_processing_odete_v5.log"

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
# These functions remain largely unchanged but are crucial.

def safe_json_dump(data: Any, default_behavior: str = "null") -> Optional[str]:
    """Safely dumps Python data to a JSON string."""
    try:
        return json.dumps(data, default=str) 
    except TypeError as e:
        logger.warning(f"TypeError during JSON dump for data snippet '{str(data)[:100]}': {e}. Returning default.")
        if default_behavior == "null": return None 
        elif default_behavior == "empty_list": return "[]"
        elif default_behavior == "empty_dict": return "{}"
        return None

def safe_json_load(json_string: Optional[str], return_type: str = "list") -> Any:
    """Safely loads a JSON string into a Python object."""
    default_return = [] if return_type == "list" else {} if return_type == "dict" else None
    if pd.isna(json_string) or not isinstance(json_string, str): return default_return
    try:
        loaded_data = json.loads(json_string)
        if return_type == "list" and not isinstance(loaded_data, list):
            return [loaded_data] if loaded_data is not None else []
        if return_type == "dict" and not isinstance(loaded_data, dict): return {} 
        return loaded_data
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"JSONDecodeError/TypeError: '{e}' for JSON string snippet: '{str(json_string)[:100]}'.")
        return default_return

def extract_worker_id_from_filename(file_path: Path) -> str:
    return file_path.stem

def parse_html_to_profile_json(html_content: str, filename: str) -> Optional[Dict[str, Any]]:
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not tag or not tag.string: return None
        data = json.loads(tag.string)
        return data.get('props', {}).get('pageProps', {}).get('profile', None)
    except Exception as e:
        logger.error(f"Error parsing HTML/JSON for {filename}: {e}", exc_info=True)
        return None

def load_all_html_files(base_dir: Path) -> List[Dict[str, Any]]:
    items = []
    if not base_dir.is_dir():
        logger.error(f"Input directory not found: {base_dir}")
        return items
    logger.info(f"Scanning HTML files in: {base_dir}")
    files = [f for f in base_dir.glob("*") if f.is_file()]
    total = len(files)
    logger.info(f"Found {total} potential files.")
    for i, fp in enumerate(files):
        if (i + 1) % 200 == 0: logger.info(f"Processing file {i+1}/{total}: {fp.name}")
        worker_id = extract_worker_id_from_filename(fp)
        try:
            with open(fp, "r", encoding="utf-8") as f: html = f.read()
            profile_json = parse_html_to_profile_json(html, fp.name)
            if profile_json:
                items.append({
                    "worker_id_from_filename": worker_id, "profile_data": profile_json,
                    "source_filename": fp.name    
                })
        except Exception as e: logger.error(f"Error processing file {fp.name}: {e}", exc_info=False)
    logger.info(f"Successfully loaded and parsed data from {len(items)}/{total} files.")
    return items

# --- Flattening Function (Largely unchanged, ensures fields are present) ---
def flatten_odete_profile(profile_item: Dict[str, Any]) -> Dict[str, Any]:
    flat_data = {"_flattening_error": False}
    profile_json = profile_item.get("profile_data", {})
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
        for field in core_fields: flat_data[f"profile_{field}"] = profile_json.get(field)

        list_fields = {
            "servicos": "profile_servicos_json", "regioes": "profile_regioes_json",
            "comentarios": "profile_comentarios_json", "solicitacoes": "profile_solicitacoes_json",
            "selos": "profile_selos_json", "hints": "profile_hints_json", "mesesPagos": "profile_mesesPagos_json"
        }
        for raw, new_name in list_fields.items():
            flat_data[new_name] = safe_json_dump(profile_json.get(raw, [])) # Default to empty list if missing

        for dt_f in ["profile_ultimoPagamento", "profile_ultimoAcesso", "profile_createdAt"]:
            if flat_data.get(dt_f): flat_data[dt_f] = pd.to_datetime(flat_data[dt_f], errors='coerce', utc=True)
        for bool_f in ["profile_enabledInstagramFeed", "profile_fullSchedule", "profile_online"]:
            val = flat_data.get(bool_f)
            flat_data[bool_f] = bool(val) if val is not None else None
        num_fields = [
            "profile_next", "profile_valor", "profile_totalDicas", "profile_totalRecomendacoes", "profile_totalMatchs", 
            "profile_totalSelos", "profile_totalPropostasVisualizadas", "profile_totalScore", 
            "profile_porcentagemContratacao", "profile_tempoMedioVisualizacaoOferta", "profile_rating"
        ]
        for num_f in num_fields:
            if flat_data.get(num_f) is not None: flat_data[num_f] = pd.to_numeric(flat_data[num_f], errors='coerce')
        
        flat_data["profile_id"] = flat_data.pop("profile__id", None)
        if flat_data["profile_id"] is None:
             logger.warning(f"Critical: Profile ID ('_id') missing for file {flat_data.get('source_filename')}.")
             flat_data["_flattening_error"] = True
    except Exception as e:
        logger.error(f"Flattening error for {flat_data.get('source_filename')}: {e}", exc_info=True)
        flat_data["_flattening_error"] = True
    return flat_data

# --- DataFrame Creation (Largely unchanged) ---
def create_aggregated_dataframe(loaded_items: List[Dict[str, Any]]) -> pd.DataFrame:
    logger.info(f"Aggregating {len(loaded_items)} items...")
    records = [flatten_odete_profile(item) for item in loaded_items if isinstance(item.get("profile_data"), dict)]
    valid_records = [r for r in records if not r.get("_flattening_error")]
    if not valid_records: return pd.DataFrame()
    df = pd.DataFrame(valid_records)
    if "_flattening_error" in df.columns: df.drop(columns=["_flattening_error"], inplace=True)
    if "profile_id" in df.columns:
        df.dropna(subset=["profile_id"], inplace=True)
        df.drop_duplicates(subset=["profile_id"], keep="first", inplace=True)
    else: logger.error("Cannot deduplicate: 'profile_id' missing.")
    try: df = df.convert_dtypes()
    except Exception as e: logger.error(f"convert_dtypes error: {e}")
    logger.info(f"Aggregated DataFrame: {len(df)} unique profiles, {len(df.columns)} columns.")
    return df

# --- Pandera Schemas ---
# Schema for the main flattened profiles DataFrame (Corrected in v4, no changes needed here for that fix)
flattened_profile_schema = DataFrameSchema(
    columns={
        "profile_id": Column(str, nullable=False, unique=True, required=True, title="Profile Primary ID"), # Original: _id
        "worker_id_from_filename": Column(str, nullable=False, description="Worker ID from filename"),
        "source_filename": Column(str, nullable=False, description="Original HTML filename"),
        "profile_nome": Column(str, nullable=True, coerce=True, description="Worker's first name"), # Original: nome
        "profile_sobrenome": Column(str, nullable=True, coerce=True, description="Worker's last name"), # Original: sobrenome
        "profile_historico": Column(str, nullable=True, coerce=True, description="Worker's biography/history text"), # Original: historico
        "profile_uf": Column(str, nullable=True, coerce=True, description="State (Unidade Federativa)"), # Original: uf
        "profile_cidade": Column(str, nullable=True, coerce=True, description="City"), # Original: cidade
        "profile_bairro": Column(str, nullable=True, coerce=True, description="Neighborhood"), # Original: bairro
        "profile_valor": Column(float, nullable=True, coerce=True, checks=Check.ge(0), description="Service price"), # Original: valor
        "profile_createdAt": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Profile creation timestamp"), # Original: createdAt
        "profile_ultimoAcesso": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Last access timestamp"), # Original: ultimoAcesso
        "profile_ultimoPagamento": Column("datetime64[ns, UTC]", nullable=True, coerce=True, description="Last payment timestamp"), # Original: ultimoPagamento
        "profile_rating": Column(float, nullable=True, coerce=True, checks=Check.in_range(0,5), description="Overall profile rating"), # Original: rating
        "profile_online": Column(pd.BooleanDtype(), nullable=True, coerce=True, description="Online status"), # Original: online
        "profile_servicos_json": Column(str, nullable=True, coerce=True, description="JSON: Services offered"), # Original: servicos
        "profile_regioes_json": Column(str, nullable=True, coerce=True, description="JSON: Regions served"), # Original: regioes
        "profile_comentarios_json": Column(str, nullable=True, coerce=True, description="JSON: Comments/Recommendations"), # Original: comentarios
        "profile_solicitacoes_json": Column(str, nullable=True, coerce=True, description="JSON: Job solicitations"), # Original: solicitacoes
        "profile_selos_json": Column(str, nullable=True, coerce=True, description="JSON: Badges/Seals"), # Original: selos
        "profile_totalRecomendacoes": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)), # Original: totalRecomendacoes
        "profile_totalMatchs": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)), # Original: totalMatchs
        "profile_totalSelos": Column(pd.Int64Dtype(), nullable=True, coerce=True, checks=Check.ge(0)), # Original: totalSelos
        "profile_totalScore": Column(pd.Int64Dtype(), nullable=True, coerce=True), # Original: totalScore
    }, strict=False, coerce=True, unique=["profile_id"])

# --- Quality Check Function (Unchanged from v4) ---
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
        try: report_df.to_csv(report_file, index=False, encoding="utf-8")
        except Exception as e: logger.error(f"Failed to save empty report for '{df_name}': {e}", exc_info=True)
        return df, False
    validated_df = df.copy(); validation_errors_df = None; validation_passed = False; validation_error_count = 0
    df_for_stats = df.copy() 
    try:
        if isinstance(validated_df.index, pd.MultiIndex) and schema.index and schema.index.unique: validated_df.reset_index(inplace=True)
        validated_df = schema.validate(validated_df, lazy=True)
        logger.info(f"Schema validation PASSED for '{df_name}'."); validation_passed = True; df_for_stats = validated_df.copy()
    except pa.errors.SchemaErrors as err:
        logger.error(f"Schema validation FAILED for '{df_name}'. Errors: {len(err.failure_cases)}")
        validation_errors_df = err.failure_cases; validation_error_count = len(validation_errors_df)
        try: logger.error(f"Validation Errors Sample (first 5):\n{json.dumps(validation_errors_df.head().to_dict(orient='records'), indent=2, default=str)}")
        except Exception: logger.error("Could not display validation error sample.")
    except Exception as verr: logger.error(f"Unexpected Pandera validation error for '{df_name}': {verr}", exc_info=True); validation_error_count = -1
    
    # ... (rest of QC function is extensive and assumed to be mostly correct from v4, slight log adjustments)
    quality_report_data = {
        "dataframe_name": df_name, "total_rows": len(df_for_stats), "total_columns": len(df_for_stats.columns),
        "schema_validation_passed": validation_passed, "validation_error_count": validation_error_count,
    }
    missing = df_for_stats.isnull().sum()
    quality_report_data["missing_values_total"] = int(missing.sum())
    quality_report_data["missing_values_per_column_json"] = safe_json_dump(missing[missing > 0].astype(int).to_dict())
    quality_report_data["data_types_per_column_json"] = safe_json_dump({col: str(df_for_stats[col].dtype) for col in df_for_stats.columns})
    
    pk_col = (schema.index.name if schema.index and schema.index.unique else 
              next((name for name, col in schema.columns.items() if col.unique), None))
    if pk_col and pk_col in df_for_stats.columns:
        quality_report_data["primary_key_null_count"] = int(df_for_stats[pk_col].isnull().sum())
        quality_report_data["duplicate_pk_values_count"] = int(df_for_stats[pk_col].dropna().duplicated().sum())
    else: quality_report_data["primary_key_null_count"] = quality_report_data["duplicate_pk_values_count"] = "N/A"

    try:
        pd.DataFrame([quality_report_data]).to_csv(output_dir / f"quality_report_{df_name}.csv", index=False)
        df_to_save = validated_df if validation_passed else df
        bool_cols = df_to_save.select_dtypes(include=[pd.BooleanDtype(), "boolean"]).columns
        for col in bool_cols: df_to_save[col] = df_to_save[col].map({True: True, False: False, pd.NA: None})
        df_to_save.to_csv(output_dir / f"{df_name}_processed.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
        if validation_errors_df is not None and not validation_errors_df.empty:
            validation_errors_df.to_csv(output_dir / f"validation_errors_{df_name}.csv", index=False)
    except Exception as e: logger.error(f"Failed to save report/data for '{df_name}': {e}"); return df, False
    logger.info(f"--- Finished QC for '{df_name}' in {time.time() - start_time_qc:.2f}s ---")
    return df_to_save, True

# --- Normalization Functions ---
def normalize_profiles_table(df_flat: pd.DataFrame, schema_cols: list) -> pd.DataFrame:
    """Creates the main 'Profiles' table with English column names."""
    logger.info("Normalizing 'Profiles' table...")
    # Mapping from flattened column name to final English name
    profile_col_mapping = {
        "profile_id": "profile_id", # PK
        "worker_id_from_filename": "worker_id_from_filename",
        "source_filename": "source_filename",
        "profile_enabledInstagramFeed": "instagram_feed_enabled", # Original: enabledInstagramFeed
        "profile_fullSchedule": "has_full_schedule",         # Original: fullSchedule
        "profile_online": "is_online",                       # Original: online
        "profile_next": "platform_next_metric",            # Original: next (meaning unclear, kept generic)
        "profile_nome": "first_name",                      # Original: nome
        "profile_sobrenome": "last_name",                   # Original: sobrenome
        "profile_historico": "biography",                   # Original: historico
        "profile_uf": "state_code",                         # Original: uf
        "profile_imagem": "profile_image_filename",         # Original: imagem
        "profile_cidade": "city",                           # Original: cidade
        "profile_bairro": "neighborhood",                   # Original: bairro
        "profile_valor": "service_price",                   # Original: valor
        "profile_ultimoPagamento": "last_payment_at",       # Original: ultimoPagamento
        "profile_ultimoAcesso": "last_accessed_at",          # Original: ultimoAcesso
        "profile_createdAt": "profile_created_at",          # Original: createdAt
        "profile_totalDicas": "total_hints",                # Original: totalDicas
        "profile_totalRecomendacoes": "total_recommendations_count", # Original: totalRecomendacoes
        "profile_totalMatchs": "total_jobs_matched_count",   # Original: totalMatchs
        "profile_totalSelos": "total_badges_count",         # Original: totalSelos
        "profile_totalPropostasVisualizadas": "total_proposals_viewed_count", # Original: totalPropostasVisualizadas
        "profile_totalScore": "platform_score",             # Original: totalScore
        "profile_porcentagemContratacao":"hiring_percentage",        # Original: porcentagemContratacao
        "profile_tempoMedioVisualizacaoOferta":"avg_offer_view_time_seconds", # Original: tempoMedioVisualizacaoOferta
        "profile_rating": "overall_profile_rating"          # Original: rating
    }
    relevant_flat_cols = [flat_col for flat_col in profile_col_mapping.keys() if flat_col in df_flat.columns]
    if "profile_id" not in relevant_flat_cols:
        logger.error("'profile_id' missing. Cannot create Profiles table.")
        return pd.DataFrame()
    
    profiles_df = df_flat[relevant_flat_cols].copy()
    profiles_df.rename(columns=profile_col_mapping, inplace=True) # Rename to English
    
    try: profiles_df.set_index("profile_id", inplace=True, verify_integrity=True)
    except Exception as e: 
        logger.error(f"Error setting 'profile_id' as index: {e}. Dropping duplicates and retrying.")
        profiles_df.drop_duplicates(subset=["profile_id"], inplace=True) # Ensure profile_id is unique before setting index
        profiles_df.set_index("profile_id", inplace=True)
        
    logger.info(f"Created 'Profiles' table with {len(profiles_df)} rows and {len(profiles_df.columns)} columns.")
    return profiles_df

def normalize_profile_services_revised(df_flat: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes services into a ProfileServices table with (profile_id, service_offered_text).
    """
    logger.info("Normalizing Profile Services (revised approach)...")
    all_services_data = []
    if "profile_id" not in df_flat.columns or "profile_servicos_json" not in df_flat.columns:
        logger.error("Missing 'profile_id' or 'profile_servicos_json' for service normalization.")
        return pd.DataFrame()

    for index, row in df_flat.iterrows():
        profile_id = row["profile_id"]
        services_list = safe_json_load(row["profile_servicos_json"], "list") # Expected: ["Cozinhar", "Faxinar"]
        if services_list:
            for service_name in services_list:
                if isinstance(service_name, str) and service_name.strip():
                    all_services_data.append({
                        "profile_id": profile_id,
                        "service_offered": service_name.strip() # Original: value from servicos list
                    })
    
    if not all_services_data:
        logger.info("No service data found for profiles.")
        return pd.DataFrame()
        
    profile_services_df = pd.DataFrame(all_services_data)
    profile_services_df.reset_index(inplace=True) # Ensure a unique PK can be made
    profile_services_df.rename(columns={'index': 'profile_service_pk'}, inplace=True) # Use index as PK
    profile_services_df["profile_service_pk"] = profile_services_df["profile_service_pk"] + 1 # Make it 1-based

    logger.info(f"Created 'ProfileServices' table with {len(profile_services_df)} service entries.")
    return profile_services_df[["profile_service_pk", "profile_id", "service_offered"]]

def normalize_recommendations(df_flat: pd.DataFrame) -> pd.DataFrame:
    """Normalizes comments/recommendations."""
    logger.info("Normalizing recommendations (comentarios)...")
    recommendations_list = []
    if "profile_id" not in df_flat.columns or "profile_comentarios_json" not in df_flat.columns:
        logger.error("Missing 'profile_id' or 'profile_comentarios_json' for recommendation normalization.")
        return pd.DataFrame()

    for index, row in df_flat.iterrows():
        profile_id = row["profile_id"] # The profile being recommended
        comments_json_list = safe_json_load(row["profile_comentarios_json"], "list")
        
        for comment_obj in comments_json_list:
            if not isinstance(comment_obj, dict): continue
            contractor_obj = comment_obj.get("contratante", {}) # Original: contratante
            
            recommendations_list.append({
                "recommendation_id": comment_obj.get("_id"),         # Original: _id (of comment)
                "profile_id": profile_id,                            # FK to Profiles table
                "message": comment_obj.get("mensagem"),              # Original: mensagem
                "recommendation_created_at": pd.to_datetime(comment_obj.get("createdAt"), errors='coerce', utc=True), # Original: createdAt (of comment)
                "contractor_id": contractor_obj.get("_id"),          # Original: contratante._id
                "contractor_name": contractor_obj.get("nome"),       # Original: contratante.nome
                "contractor_image_url": contractor_obj.get("imagem") # Original: contratante.imagem
            })
    
    if not recommendations_list:
        logger.info("No recommendation data found.")
        return pd.DataFrame()
        
    recommendations_df = pd.DataFrame(recommendations_list)
    recommendations_df.dropna(subset=["recommendation_id"], inplace=True) # PK cannot be null
    recommendations_df.drop_duplicates(subset=["recommendation_id"], keep="first", inplace=True)
    logger.info(f"Created 'Recommendations' table with {len(recommendations_df)} entries.")
    return recommendations_df

def normalize_badges(df_flat: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes badges (selos) into BadgeTypes and ProfileBadges tables."""
    logger.info("Normalizing badges (selos)...")
    badge_types_dict = {} # To store unique badge types: {badge_type_id: {details}}
    profile_badges_list = []

    if "profile_id" not in df_flat.columns or "profile_selos_json" not in df_flat.columns:
        logger.error("Missing 'profile_id' or 'profile_selos_json' for badge normalization.")
        return pd.DataFrame(), pd.DataFrame()

    for index, row in df_flat.iterrows():
        profile_id = row["profile_id"]
        selos_json_list = safe_json_load(row["profile_selos_json"], "list")

        for selo_instance_obj in selos_json_list:
            if not isinstance(selo_instance_obj, dict): continue
            
            profile_badge_id = selo_instance_obj.get("_id") # This is the ID of the badge instance for the profile
            selo_type_obj = selo_instance_obj.get("selo")    # Original: selo (nested object)
            
            if not isinstance(selo_type_obj, dict) or not profile_badge_id: continue
            
            badge_type_id = selo_type_obj.get("_id") # Original: selo._id
            if not badge_type_id: continue

            # Populate BadgeTypes master
            if badge_type_id not in badge_types_dict:
                badge_types_dict[badge_type_id] = {
                    "badge_type_id": badge_type_id,
                    "badge_title": selo_type_obj.get("title"),          # Original: selo.title
                    "badge_description": selo_type_obj.get("description"),# Original: selo.description
                    "badge_image_url": selo_type_obj.get("imagem")      # Original: selo.imagem
                }
            
            # Populate ProfileBadges junction
            profile_badges_list.append({
                "profile_badge_id": profile_badge_id, # PK for this instance
                "profile_id": profile_id,             # FK
                "badge_type_id": badge_type_id        # FK
            })

    badge_types_df = pd.DataFrame(list(badge_types_dict.values()))
    profile_badges_df = pd.DataFrame(profile_badges_list)
    
    if not badge_types_df.empty:
        badge_types_df.drop_duplicates(subset=["badge_type_id"], inplace=True)
        logger.info(f"Created 'BadgeTypes' table with {len(badge_types_df)} unique badge types.")
    else: logger.info("No badge type data found.")
        
    if not profile_badges_df.empty:
        profile_badges_df.drop_duplicates(subset=["profile_badge_id"], inplace=True)
        logger.info(f"Created 'ProfileBadges' table with {len(profile_badges_df)} badge assignments.")
    else: logger.info("No profile badge assignments found.")
        
    return badge_types_df, profile_badges_df

def normalize_job_solicitations_and_evaluations(df_flat: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes job solicitations and their evaluations."""
    logger.info("Normalizing job solicitations (solicitacoes) and evaluations...")
    solicitations_list = []
    evaluation_criteria_list = []
    eval_criterion_pk_counter = 1

    required_cols = ["profile_id", "profile_solicitacoes_json"]
    if not all(col in df_flat.columns for col in required_cols):
        logger.error(f"Missing one of {required_cols} for solicitation normalization.")
        return pd.DataFrame(), pd.DataFrame()

    for index, row in df_flat.iterrows():
        profile_id = row["profile_id"] # The worker's profile
        solicitacoes_json_list = safe_json_load(row["profile_solicitacoes_json"], "list")

        for solic_obj in solicitacoes_json_list:
            if not isinstance(solic_obj, dict): continue
            
            solicitation_id = solic_obj.get("_id") # Original: _id (of solicitation)
            if not solicitation_id: continue

            contractor_obj = solic_obj.get("contratante", {})   # Original: contratante
            evaluation_obj = solic_obj.get("avaliacaodoContratante", {}) # Original: avaliacaodoContratante
            
            solicitations_list.append({
                "solicitation_id": solicitation_id,
                "profile_id": profile_id,
                "solicitation_created_at": pd.to_datetime(solic_obj.get("createdAt"), errors='coerce', utc=True), # Original: createdAt
                "contractor_match": solic_obj.get("matchContratante"), # Original: matchContratante
                "contractor_message": solic_obj.get("messageContratante"), # Original: messageContratante
                "worker_match": solic_obj.get("matchColaborador"),         # Original: matchColaborador
                "contractor_id": contractor_obj.get("_id"),
                "contractor_name": contractor_obj.get("nome"),
                "contractor_image_url": contractor_obj.get("imagem"),
                "client_overall_rating_for_job": pd.to_numeric(evaluation_obj.get("rating"), errors='coerce') # Original: avaliacaodoContratante.rating
            })

            # Normalize evaluation criteria for this solicitation
            if isinstance(evaluation_obj, dict):
                criteria_names = evaluation_obj.get("criterios", []) # Original: criterios (list of strings)
                criteria_stars = evaluation_obj.get("stars", [])     # Original: stars (list of numbers)
                
                if isinstance(criteria_names, list) and isinstance(criteria_stars, list) and len(criteria_names) == len(criteria_stars):
                    for i, criterion_name in enumerate(criteria_names):
                        evaluation_criteria_list.append({
                            "job_evaluation_criterion_pk": eval_criterion_pk_counter,
                            "solicitation_id": solicitation_id, # FK
                            "criterion_name": criterion_name,
                            "criterion_stars": pd.to_numeric(criteria_stars[i], errors='coerce')
                        })
                        eval_criterion_pk_counter += 1
    
    solicitations_df = pd.DataFrame(solicitations_list)
    evaluation_criteria_df = pd.DataFrame(evaluation_criteria_list)

    if not solicitations_df.empty:
        solicitations_df.dropna(subset=["solicitation_id"], inplace=True)
        solicitations_df.drop_duplicates(subset=["solicitation_id"], keep="first", inplace=True)
        logger.info(f"Created 'JobSolicitations' table with {len(solicitations_df)} entries.")
    else: logger.info("No job solicitation data found.")

    if not evaluation_criteria_df.empty:
        logger.info(f"Created 'JobEvaluationCriteria' table with {len(evaluation_criteria_df)} entries.")
    else: logger.info("No job evaluation criteria data found.")
        
    return solicitations_df, evaluation_criteria_df

# --- Main Execution Block ---
def run_data_processing():
    overall_start_time = time.time()
    logger.info(f"--- Starting Odete.com.br Data Processing (v5) ---")
    # ... (load_all_html_files, create_aggregated_dataframe, initial QC steps are the same as v4) ...
    start_time_load = time.time()
    all_profile_items = load_all_html_files(BASE_RAW_DIR)
    logger.info(f"HTML loading and initial parsing took {time.time() - start_time_load:.2f} seconds.")
    if not all_profile_items: sys.exit(1)

    start_time_flatten_agg = time.time()
    profiles_flat_df = create_aggregated_dataframe(all_profile_items)
    logger.info(f"Flattening and aggregation took {time.time() - start_time_flatten_agg:.2f} seconds.")
    if profiles_flat_df.empty: sys.exit(1)
    
    logger.info("--- Initial QC on Flattened Data ---")
    profiles_flat_validated_df, flat_qc_passed = run_quality_checks_and_report(
        profiles_flat_df, flattened_profile_schema, "flattened_profiles", BASE_PREPROCESSED_DIR
    )
    if not flat_qc_passed and profiles_flat_validated_df.empty: sys.exit(1)
    elif not flat_qc_passed: logger.warning("Initial QC failed. Proceeding with unvalidated data.")
    else: logger.info("Initial QC passed for flattened_profiles.")
    
    logger.info("--- Starting Data Normalization (v5) ---")
    start_time_norm = time.time()
    normalized_tables = {}
    
    # A. Normalize 'Profiles' table
    final_profile_table_columns = [
        "profile_id", "worker_id_from_filename", "source_filename", "profile_enabledInstagramFeed",
        "profile_fullSchedule", "profile_online", "profile_next", "profile_nome", "profile_sobrenome",
        "profile_historico", "profile_uf", "profile_imagem", "profile_cidade", "profile_bairro",
        "profile_valor", "profile_ultimoPagamento", "profile_ultimoAcesso", "profile_createdAt",
        "profile_totalDicas", "profile_totalRecomendacoes", "profile_totalMatchs", "profile_totalSelos",
        "profile_totalPropostasVisualizadas", "profile_totalScore", "profile_porcentagemContratacao",
        "profile_tempoMedioVisualizacaoOferta", "profile_rating"
    ] # These are keys from the FLATTENED df
    normalized_tables["Profiles"] = normalize_profiles_table(profiles_flat_validated_df, final_profile_table_columns)
    
    # B. Normalize 'ProfileServices' (Revised approach)
    normalized_tables["ProfileServices"] = normalize_profile_services_revised(profiles_flat_validated_df)
    
    # C. Normalize 'Recommendations'
    normalized_tables["Recommendations"] = normalize_recommendations(profiles_flat_validated_df)

    # D. Normalize 'Badges'
    badge_types_df, profile_badges_df = normalize_badges(profiles_flat_validated_df)
    normalized_tables["BadgeTypes"] = badge_types_df
    normalized_tables["ProfileBadges"] = profile_badges_df

    # E. Normalize 'JobSolicitations' and 'JobEvaluationCriteria'
    solicitations_df, evaluation_criteria_df = normalize_job_solicitations_and_evaluations(profiles_flat_validated_df)
    normalized_tables["JobSolicitations"] = solicitations_df
    normalized_tables["JobEvaluationCriteria"] = evaluation_criteria_df
    
    logger.info(f"Normalization phase took {time.time() - start_time_norm:.2f} seconds.")

    # --- QC and Export Normalized Tables ---
    logger.info("--- QC and Export Normalized Tables (v5) ---")
    
    # Define Pandera Schemas for ALL normalized tables (with English names)
    profiles_norm_schema = DataFrameSchema( # For the main 'Profiles' table AFTER normalization and rename
        columns={
            "worker_id_from_filename": Column(str, nullable=False), 
            "source_filename": Column(str, nullable=False),
            "instagram_feed_enabled": Column(pd.BooleanDtype(), nullable=True), # Original: enabledInstagramFeed
            "has_full_schedule": Column(pd.BooleanDtype(), nullable=True), # Original: fullSchedule
            "is_online": Column(pd.BooleanDtype(), nullable=True), # Original: online
            "platform_next_metric": Column(pd.Int64Dtype(), nullable=True), # Original: next
            "first_name": Column(str, nullable=True), # Original: nome
            "last_name": Column(str, nullable=True), # Original: sobrenome
            "biography": Column(str, nullable=True), # Original: historico
            "state_code": Column(str, nullable=True), # Original: uf
            "profile_image_filename": Column(str, nullable=True), # Original: imagem
            "city": Column(str, nullable=True), # Original: cidade
            "neighborhood": Column(str, nullable=True), # Original: bairro
            "service_price": Column(float, nullable=True, checks=Check.ge(0)), # Original: valor
            "last_payment_at": Column("datetime64[ns, UTC]", nullable=True), # Original: ultimoPagamento
            "last_accessed_at": Column("datetime64[ns, UTC]", nullable=True), # Original: ultimoAcesso
            "profile_created_at": Column("datetime64[ns, UTC]", nullable=True), # Original: createdAt
            "total_hints": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: totalDicas (assuming it exists)
            "total_recommendations_count": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: totalRecomendacoes
            "total_jobs_matched_count": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: totalMatchs
            "total_badges_count": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: totalSelos
            "total_proposals_viewed_count": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: totalPropostasVisualizadas
            "platform_score": Column(pd.Int64Dtype(), nullable=True), # Original: totalScore
            "hiring_percentage": Column(float, nullable=True, checks=Check.ge(0)), # Original: porcentagemContratacao
            "avg_offer_view_time_seconds": Column(pd.Int64Dtype(), nullable=True, checks=Check.ge(0)), # Original: tempoMedioVisualizacaoOferta
            "overall_profile_rating": Column(float, nullable=True, checks=Check.in_range(0,5)), # Original: rating
        }, index=Index(str, name="profile_id", unique=True, nullable=False), strict=False, coerce=True )

    profile_services_schema = DataFrameSchema({
        "profile_service_pk": Column(int, unique=True, nullable=False, required=True),
        "profile_id": Column(str, nullable=False),       # FK to Profiles.profile_id
        "service_offered": Column(str, nullable=False)   # Original: value from servicos list
    }, strict=True, coerce=True)

    recommendations_schema = DataFrameSchema({
        "recommendation_id": Column(str, unique=True, nullable=False), # Original: _id from comentario
        "profile_id": Column(str, nullable=False),                  # FK
        "message": Column(str, nullable=True),                      # Original: mensagem
        "recommendation_created_at": Column("datetime64[ns, UTC]", nullable=True), # Original: createdAt from comentario
        "contractor_id": Column(str, nullable=True),                # Original: contratante._id
        "contractor_name": Column(str, nullable=True),              # Original: contratante.nome
        "contractor_image_url": Column(str, nullable=True)          # Original: contratante.imagem
    }, strict=True, coerce=True)

    badge_types_schema = DataFrameSchema({
        "badge_type_id": Column(str, unique=True, nullable=False), # Original: selo._id
        "badge_title": Column(str, nullable=True),                 # Original: selo.title
        "badge_description": Column(str, nullable=True),           # Original: selo.description
        "badge_image_url": Column(str, nullable=True)              # Original: selo.imagem
    }, strict=True, coerce=True)

    profile_badges_schema = DataFrameSchema({
        "profile_badge_id": Column(str, unique=True, nullable=False), # Original: _id from selos array entry
        "profile_id": Column(str, nullable=False),                 # FK
        "badge_type_id": Column(str, nullable=False)               # FK to BadgeTypes.badge_type_id
    }, strict=True, coerce=True)

    job_solicitations_schema = DataFrameSchema({
        "solicitation_id": Column(str, unique=True, nullable=False), # Original: _id from solicitacao
        "profile_id": Column(str, nullable=False),                   # FK
        "solicitation_created_at": Column("datetime64[ns, UTC]", nullable=True), # Original: createdAt
        "contractor_match": Column(pd.BooleanDtype(), nullable=True), # Original: matchContratante
        "contractor_message": Column(str, nullable=True),             # Original: messageContratante
        "worker_match": Column(pd.BooleanDtype(), nullable=True),     # Original: matchColaborador
        "contractor_id": Column(str, nullable=True),
        "contractor_name": Column(str, nullable=True),
        "contractor_image_url": Column(str, nullable=True),
        "client_overall_rating_for_job": Column(float, nullable=True, checks=Check.in_range(0,5)) # Original: avaliacaodoContratante.rating
    }, strict=True, coerce=True)

    job_evaluation_criteria_schema = DataFrameSchema({
        "job_evaluation_criterion_pk": Column(int, unique=True, nullable=False),
        "solicitation_id": Column(str, nullable=False), # FK
        "criterion_name": Column(str, nullable=False),  # Original: value from criterios list
        "criterion_stars": Column(float, nullable=True, checks=Check.in_range(0,5)) # Original: value from stars list (assuming 0-5)
    }, strict=True, coerce=True)

    # Master dictionary of schemas for normalized tables
    schemas_for_normalized_tables = {
         "Profiles": profiles_norm_schema, 
         "ProfileServices": profile_services_schema,
         "Recommendations": recommendations_schema,
         "BadgeTypes": badge_types_schema,
         "ProfileBadges": profile_badges_schema,
         "JobSolicitations": job_solicitations_schema,
         "JobEvaluationCriteria": job_evaluation_criteria_schema,
    }

    for table_name, norm_df in normalized_tables.items():
        logger.info(f"--- QC & Export for Normalized Table: '{table_name}' ---")
        if norm_df.empty:
            logger.info(f"Normalized table '{table_name}' is empty. Creating empty report/file.")
            (BASE_PREPROCESSED_DIR / f"{table_name}_processed.csv").write_text("") # Create empty file
            run_quality_checks_and_report(pd.DataFrame(), DataFrameSchema(name=f"empty_{table_name}"), table_name, BASE_PREPROCESSED_DIR)
            continue
        
        schema_to_use = schemas_for_normalized_tables.get(table_name)
        if schema_to_use:
            df_for_validation = norm_df
            # Align DataFrame index with schema expectation for validation
            is_pk_in_index = schema_to_use.index and schema_to_use.index.name and norm_df.index.name == schema_to_use.index.name
            is_pk_col_in_df = schema_to_use.index and schema_to_use.index.name and schema_to_use.index.name in norm_df.columns

            if schema_to_use.index and schema_to_use.index.name: # Schema defines an index
                if norm_df.index.name != schema_to_use.index.name: # DF's index name differs or is None
                    if schema_to_use.index.name in norm_df.columns: # If schema's index is a column in DF
                        logger.info(f"Setting index to '{schema_to_use.index.name}' for table '{table_name}' for validation.")
                        try: df_for_validation = norm_df.set_index(schema_to_use.index.name, verify_integrity=True)
                        except Exception as e_set_idx: logger.error(f"Could not set index '{schema_to_use.index.name}' for '{table_name}': {e_set_idx}")
                    # else: PK column not available to set as index, proceed with current DF structure. Pandera might error.
            elif not schema_to_use.index and norm_df.index.name is not None: # Schema has no index, DF has one
                 df_for_validation = norm_df.reset_index()
            
            validated_df, qc_success = run_quality_checks_and_report(df_for_validation, schema_to_use, table_name, BASE_PREPROCESSED_DIR)
            if not qc_success: logger.error(f"QC or saving failed for '{table_name}'.")
        else: # No schema defined
            logger.warning(f"No schema for '{table_name}'. Saving without validation.")
            try:
                output_path = BASE_PREPROCESSED_DIR / f"{table_name}_processed.csv"
                save_idx = bool(norm_df.index.name and norm_df.index.name not in ["index", None]) # Save meaningful index
                norm_df.to_csv(output_path, index=save_idx, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
                logger.info(f"Saved unvalidated table '{table_name}' to: {output_path}")
            except Exception as e: logger.error(f"Failed to save unvalidated '{table_name}': {e}", exc_info=True)
                
    overall_duration = time.time() - overall_start_time
    logger.info(f"--- Odete.com.br Data Processing Finished in {overall_duration:.2f} seconds ---")

if __name__ == "__main__":
    run_data_processing()