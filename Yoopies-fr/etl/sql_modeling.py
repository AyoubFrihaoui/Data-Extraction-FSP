# src/etl/yoopies_load_postgres_fresh.py

import sys
import json
import logging
from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple, Any, Optional  # Import Optional
import traceback

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    ForeignKey,
    Text,
    TIMESTAMP,
    Numeric,
    UniqueConstraint,
    Index,
    MetaData,  # Import MetaData
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.exc import (
    IntegrityError,
    SQLAlchemyError,
    ProgrammingError,
)  # Import ProgrammingError
from sqlalchemy.dialects.postgresql import (
    JSONB,
)  # Keep for potential future use, though not strictly needed for these CSVs

# --- Configuration ---
logger = logging.getLogger(__name__)
try:
    # Ensure config.py has the correct variable name for the password
    from config import YOOPIES_FR_PASSWORD

    DATABASE_URI = (
        f"postgresql://postgres:{YOOPIES_FR_PASSWORD}@localhost:5432/yoopies-fr"
    )
except ImportError:
    logger.error("Error: config.py not found or variable YOOPIES_FR_PASSWORD missing.")
    DATABASE_URI = None

# Input directory for processed CSVs
BASE_PREPROCESSED_DIR = Path("preprocessed_data/yoopies")

# --- Logging Setup ---
LOG_FILE = BASE_PREPROCESSED_DIR / "etl_load_postgres_fresh.log"  # New log file name
if not LOG_FILE.parent.exists():
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"Created dir: {LOG_FILE.parent}")
    except Exception as e:
        print(f"FATAL: Could not create log dir {LOG_FILE.parent}: {e}")
        sys.exit(1)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# --- SQLAlchemy Setup ---
Base = declarative_base()
engine = None
SessionLocal = None

if DATABASE_URI:
    try:
        engine = create_engine(DATABASE_URI, echo=False, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logger.info("Database engine created successfully.")
        with engine.connect() as connection:
            logger.info("Database connection successful.")
    except Exception as e:
        logger.error(f"Failed to create database engine or connect: {e}")
        engine = None
else:
    logger.error("DATABASE_URI not configured. Exiting.")
    sys.exit(1)


# --- Helper Functions for Safe Data Conversion ---
def safe_int(value) -> Optional[int]:  # Use Optional Type Hinting
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def safe_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_decimal(value, default=None) -> Optional[Decimal]:
    if pd.isna(value):
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def safe_bool(value) -> Optional[bool]:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value  # Already boolean
    if isinstance(value, (int, float)):
        return bool(value)  # Handle 0/1
    if isinstance(value, str):
        low_val = value.lower().strip()
        if low_val in ["true", "t", "1", "yes", "y"]:
            return True
        if low_val in ["false", "f", "0", "no", "n"]:
            return False
    # If unsure, return None or log a warning
    logger.debug(f"Could not reliably convert '{value}' to bool, returning None.")
    return None


def safe_timestamp(value) -> Optional[pd.Timestamp]:  # Type hint correct
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime()  # Convert to standard datetime


# --- SQLAlchemy ORM Models (Based on CSV structures) ---
# These definitions MUST match the tables you expect in the DB


class User(Base):
    __tablename__ = "users"
    # Match CSV column names directly
    user_id = Column(Integer, primary_key=True)
    user_enabled = Column(Boolean, nullable=True)
    user_baseType = Column(String, nullable=True)
    user_isVerified = Column(Boolean, nullable=True)
    user_firstName = Column(String, nullable=True)
    user_lastName = Column(String, nullable=True)
    user_applicant_hasAEStatus = Column(Boolean, nullable=True)
    user_applicant_age = Column(Integer, nullable=True)
    user_grades_count = Column(Integer, nullable=True)
    user_grades_average = Column(Float, nullable=True)

    # Relationships defined for ORM convenience, data loaded separately usually
    ads = relationship(
        "Ad", back_populates="user", lazy="dynamic"
    )  # Use dynamic for large collections
    roles = relationship(
        "UserRole", back_populates="user", cascade="all, delete-orphan", lazy="dynamic"
    )
    reviews_received = relationship(
        "Review",
        back_populates="reviewed_user",
        foreign_keys="Review.reviewed_user_id",
        lazy="dynamic",
    )

    __table_args__ = (Index("ix_users_basetype", "user_baseType"),)


class Ad(Base):
    __tablename__ = "ads"
    # Match CSV column names directly
    ad_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=True, index=True)
    postal_code = Column(
        String, nullable=True, index=True
    )  # Allow NULL if CSV might have it
    care_type = Column(String, nullable=True, index=True)
    sub_type = Column(String, nullable=True, index=True)
    ad_title = Column(Text, nullable=True)
    ad_content = Column(Text, nullable=True)
    ad_type = Column(String, nullable=True)
    ad_status = Column(String, nullable=True)
    ad_createdAt = Column(TIMESTAMP(timezone=False), nullable=True)
    ad_updatedAt = Column(TIMESTAMP(timezone=False), nullable=True)
    ad_experienceAmount = Column(String, nullable=True)
    address_latitude = Column(Float, nullable=True)
    address_longitude = Column(Float, nullable=True)
    address_city = Column(String, nullable=True)
    address_zipCode = Column(String, nullable=True)

    # Relationships defined for ORM convenience
    user = relationship(
        "User", back_populates="ads", lazy="selectin"
    )  # selectin potentially better for parent
    badges = relationship(
        "AdBadge", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )
    categories = relationship(
        "AdCategory", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )
    languages = relationship(
        "AdLanguage", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )
    rates = relationship(
        "AdRate", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )
    tags = relationship(
        "AdTag", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )
    reviews = relationship(
        "Review", back_populates="ad", cascade="all, delete-orphan", lazy="dynamic"
    )

    __table_args__ = (
        Index("ix_ads_user_id", "user_id"),  # Index should match model def
        Index("ix_ads_location", "postal_code", "address_city"),
        Index("ix_ads_type_status", "ad_type", "ad_status"),
    )


class AdBadge(Base):
    __tablename__ = "ad_badges"
    # Match CSV column names directly
    ad_badge_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="CASCADE"), nullable=False, index=True
    )
    badge_name = Column(String, nullable=True)  # Allow null if needed
    badge_additionalData = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="badges")

    __table_args__ = (
        UniqueConstraint("ad_id", "badge_name", name="uq_ad_badge"),
        Index("ix_ad_badges_name", "badge_name"),
    )


class AdCategory(Base):
    __tablename__ = "ad_categories"
    # Match CSV column names directly
    ad_category_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="CASCADE"), nullable=False, index=True
    )
    service = Column(String, nullable=True)
    category = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="categories")

    __table_args__ = (
        UniqueConstraint("ad_id", "service", "category", name="uq_ad_category"),
        Index("ix_ad_categories_service_category", "service", "category"),
    )


class AdLanguage(Base):
    __tablename__ = "ad_languages"
    # Match CSV column names directly
    ad_language_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="CASCADE"), nullable=False, index=True
    )
    language_code = Column(String(10), nullable=True)

    ad = relationship("Ad", back_populates="languages")

    __table_args__ = (
        UniqueConstraint("ad_id", "language_code", name="uq_ad_language"),
        Index("ix_ad_languages_code", "language_code"),
    )


class AdRate(Base):
    __tablename__ = "ad_rates"
    # Match CSV column names directly
    ad_rate_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="CASCADE"), nullable=False, index=True
    )
    employment_type = Column(String, nullable=True)
    care_type = Column(String, nullable=True)
    rate_type = Column(String, nullable=True)
    rate_amount = Column(Numeric(10, 2), nullable=True)
    rate_timeUnit = Column(String, nullable=True)
    rate_commission = Column(Numeric(10, 2), nullable=True)

    ad = relationship("Ad", back_populates="rates")

    __table_args__ = (
        UniqueConstraint(
            "ad_id", "employment_type", "care_type", "rate_type", name="uq_ad_rate"
        ),
    )


class AdTag(Base):
    __tablename__ = "ad_tags"
    # Match CSV column names directly
    ad_tag_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="CASCADE"), nullable=False, index=True
    )
    tag_type = Column(String, nullable=True)  # Allow null if needed
    tag_value = Column(String, nullable=True)  # Allow null if needed

    ad = relationship("Ad", back_populates="tags")

    __table_args__ = (
        UniqueConstraint("ad_id", "tag_type", "tag_value", name="uq_ad_tag"),
        Index("ix_ad_tags_type_value", "tag_type", "tag_value"),
    )


class UserRole(Base):
    __tablename__ = "user_roles"
    # Match CSV column names directly (adjust if CSV has 'user_role_pk')
    # Assuming CSV structure: pk, user_id, role_name
    user_role_pk = Column(Integer, primary_key=True)
    user_id = Column(
        Integer,
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    role_name = Column(String, nullable=True)  # Allow null if needed

    user = relationship("User", back_populates="roles")

    __table_args__ = (
        UniqueConstraint("user_id", "role_name", name="uq_user_role"),
        Index("ix_user_roles_name", "role_name"),
    )


class Review(Base):
    __tablename__ = "reviews"
    # Match CSV column names directly
    review_pk = Column(Integer, primary_key=True)
    ad_id = Column(
        Integer, ForeignKey("ads.ad_id", ondelete="SET NULL"), nullable=True, index=True
    )
    reviewed_user_id = Column(
        Integer,
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    reviewer_firstName = Column(String, nullable=True)
    reviewer_lastName = Column(String, nullable=True)
    review_content = Column(Text, nullable=True)
    review_generalGrade = Column(Integer, nullable=True)
    review_experienceName = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="reviews")
    reviewed_user = relationship(
        "User", back_populates="reviews_received", foreign_keys=[reviewed_user_id]
    )


# --- Data Loading Functions ---


def load_parent_table(
    session: Session, df: pd.DataFrame, model_class: type, pk_col: str
):
    """Loads data into parent tables (User, Ad, Review) using upsert logic."""
    model_name = model_class.__tablename__
    logger.info(f"Starting load for {model_name} ({len(df)} rows from CSV)")
    loaded_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0
    model_columns = {c.name for c in model_class.__table__.columns}

    # Convert PK to appropriate type for querying
    pk_type = getattr(model_class, pk_col).type
    df[pk_col] = df[pk_col].apply(
        safe_int if isinstance(pk_type, Integer) else str
    )  # Basic example

    for idx, row in df.iterrows():
        pk_value = row.get(pk_col)
        if pd.isna(pk_value):
            skipped_count += 1
            continue

        try:
            instance = (
                session.query(model_class)
                .filter(getattr(model_class, pk_col) == pk_value)
                .one_or_none()
            )
            data_dict = {}
            # Prepare data dict with safe conversions FOR ALL COLUMNS IN MODEL
            for col_name in model_columns:
                if col_name in row.index:  # Check if column exists in DataFrame row
                    raw_value = row[col_name]
                    model_col_type = getattr(model_class, col_name).type
                    safe_value = None
                    if isinstance(model_col_type, Integer):
                        safe_value = safe_int(raw_value)
                    elif isinstance(model_col_type, Float):
                        safe_value = safe_float(raw_value)
                    elif isinstance(model_col_type, Numeric):
                        safe_value = safe_decimal(raw_value)
                    elif isinstance(model_col_type, Boolean):
                        safe_value = safe_bool(raw_value)
                    elif isinstance(model_col_type, TIMESTAMP):
                        safe_value = safe_timestamp(raw_value)
                    elif isinstance(model_col_type, (String, Text)):
                        safe_value = (
                            str(raw_value).strip() if pd.notna(raw_value) else None
                        )
                    else:
                        safe_value = raw_value
                    data_dict[col_name] = safe_value
                # else: data_dict[col_name] = None # Or handle missing columns differently

            if instance:  # Update existing instance
                updated = False
                for col, value in data_dict.items():
                    if col != pk_col and getattr(instance, col) != value:
                        setattr(instance, col, value)
                        updated = True
                if updated:
                    updated_count += 1
            else:  # Insert new instance
                # Ensure PK is in dict for insert
                if pk_col not in data_dict:
                    data_dict[pk_col] = pk_value
                # Remove None values if column is not nullable? Usually DB handles this.
                # data_dict = {k: v for k, v in data_dict.items() if v is not None} # Optional: Remove None values
                instance = model_class(**data_dict)
                session.add(instance)
                loaded_count += 1

        except SQLAlchemyError as e:
            logger.error(f"DB Error {idx}/{pk_value} in {model_name}: {e}")
            session.rollback()
            error_count += 1
        except Exception as e:
            logger.error(
                f"General Error {idx}/{pk_value} in {model_name}: {e}", exc_info=True
            )
            session.rollback()
            error_count += 1

    try:
        session.commit()
    except Exception as commit_err:
        logger.error(f"Commit error {model_name}: {commit_err}")
        session.rollback()
        error_count += 1  # Count as error
    logger.info(
        f"{model_name} Load Done. Inserted:{loaded_count}, Updated:{updated_count}, Skipped:{skipped_count}, Errors:{error_count}"
    )


def load_child_table(
    session: Session,
    df: pd.DataFrame,
    model_class: type,
    pk_col: str,
    fk_col: str,
    parent_model: type,
):
    """Loads data into child tables with FK, handling duplicates via constraints."""
    model_name = model_class.__tablename__
    logger.info(f"Starting load for {model_name} ({len(df)} rows)")
    inserted_count = 0
    skipped_fk_missing = 0
    skipped_duplicate = 0
    error_count = 0
    model_columns = {c.name for c in model_class.__table__.columns}

    # Pre-fetch valid parent IDs for efficiency (optional but good for large datasets)
    valid_parent_ids = set()
    parent_pk_col = getattr(
        parent_model, fk_col
    ).property.target.name  # Get the actual PK name from relationship
    if parent_pk_col:
        parent_ids_in_df = df[fk_col].dropna().unique()
        if len(parent_ids_in_df) > 0:
            query = session.query(getattr(parent_model, parent_pk_col))
            # Ensure type compatibility for 'in_'
            try:
                parent_ids_in_df = [
                    (
                        int(p)
                        if isinstance(
                            getattr(parent_model, parent_pk_col).type, Integer
                        )
                        else str(p)
                    )
                    for p in parent_ids_in_df
                ]
                valid_parent_ids = {
                    p[0]
                    for p in query.filter(
                        getattr(parent_model, parent_pk_col).in_(parent_ids_in_df)
                    ).all()
                }
            except Exception as e:
                logger.error(
                    f"Error pre-fetching parent IDs for {model_name}: {e}. Proceeding without pre-fetch."
                )
        logger.info(
            f"Pre-fetched {len(valid_parent_ids)} valid parent IDs for {model_name}"
        )

    for idx, row in df.iterrows():
        fk_value = row.get(fk_col)
        if pd.isna(fk_value):
            skipped_fk_missing += 1
            continue

        # Convert FK to appropriate type
        fk_type = getattr(model_class, fk_col).type
        safe_fk_value = (
            safe_int(fk_value) if isinstance(fk_type, Integer) else str(fk_value)
        )

        # Check against pre-fetched parent IDs if available
        if valid_parent_ids and safe_fk_value not in valid_parent_ids:
            skipped_fk_missing += 1
            continue
        # Optional: Query parent if not pre-fetched (slower)
        # elif not valid_parent_ids:
        #     parent_exists = session.query(parent_model).filter(getattr(parent_model, parent_pk_col) == safe_fk_value).count() > 0
        #     if not parent_exists:
        #         skipped_fk_missing += 1
        #         continue

        try:
            data_dict = {}
            for col_name in model_columns:
                if col_name in row.index:
                    raw_value = row[col_name]
                    model_col_type = getattr(model_class, col_name).type
                    safe_value = None
                    if isinstance(model_col_type, Integer):
                        safe_value = safe_int(raw_value)
                    elif isinstance(model_col_type, Float):
                        safe_value = safe_float(raw_value)
                    elif isinstance(model_col_type, Numeric):
                        safe_value = safe_decimal(raw_value)
                    elif isinstance(model_col_type, Boolean):
                        safe_value = safe_bool(raw_value)
                    elif isinstance(model_col_type, TIMESTAMP):
                        safe_value = safe_timestamp(raw_value)
                    elif isinstance(model_col_type, (String, Text)):
                        safe_value = (
                            str(raw_value).strip() if pd.notna(raw_value) else None
                        )
                    else:
                        safe_value = raw_value
                    data_dict[col_name] = safe_value
                # Handle generated PK if present in df but not needed for insert
                if col_name == pk_col and pk_col in data_dict:
                    del data_dict[pk_col]  # Let the DB generate the PK

            # Ensure FK is set correctly
            data_dict[fk_col] = safe_fk_value

            instance = model_class(**data_dict)
            session.add(instance)
            session.flush()  # Flush to catch integrity errors early
            inserted_count += 1

        except IntegrityError as e:
            session.rollback()  # Rollback the failed insert
            # Check if it's a duplicate key violation (common)
            if "duplicate key value violates unique constraint" in str(e).lower():
                skipped_duplicate += 1
                # logger.debug(f"Skipping duplicate row {idx} for {model_name}")
            else:
                logger.error(f"IntegrityError on row {idx} for {model_name}: {e}")
                error_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error on row {idx} for {model_name}: {e}")
            session.rollback()
            error_count += 1
        except Exception as e:
            logger.error(
                f"General Error on row {idx} for {model_name}: {e}", exc_info=True
            )
            session.rollback()
            error_count += 1

    try:
        session.commit()
    except Exception as commit_err:
        logger.error(f"Commit error {model_name}: {commit_err}")
        session.rollback()
        error_count += 1
    logger.info(
        f"{model_name} Load Done. Inserted:{inserted_count}, Skip_FK_Miss:{skipped_fk_missing}, Skip_Dup:{skipped_duplicate}, Errors:{error_count}"
    )


# --- Main ETL Function ---
def run_etl():
    if not engine or not SessionLocal:
        logger.error("Database engine/session not configured. Exiting.")
        sys.exit(1)

    # --- IMPORTANT: Schema Management ---
    # This script ASSUMES tables exist. Run schema creation separately if needed.
    # Example (run once manually or via a setup script):
    # logger.info("Creating schema if it doesn't exist...")
    # Base.metadata.create_all(bind=engine)
    # logger.info("Schema setup check complete.")

    session = SessionLocal()
    logger.info("DB session created for data loading.")

    # Simplified Load Order: Parent tables first, then child tables
    load_tasks = [
        # Parents
        {
            "model": User,
            "csv": "users_processed.csv",
            "pk": "user_id",
            "type": "parent",
        },
        {"model": Ad, "csv": "ads_processed.csv", "pk": "ad_id", "type": "parent"},
        {
            "model": Review,
            "csv": "reviews_processed.csv",
            "pk": "review_pk",
            "type": "parent",
        },  # Treat Review as parent initially
        # Children / Linking Tables
        {
            "model": AdBadge,
            "csv": "ad_badges_processed.csv",
            "pk": "ad_badge_pk",
            "fk": "ad_id",
            "parent": Ad,
            "type": "child",
        },
        {
            "model": AdCategory,
            "csv": "ad_categories_processed.csv",
            "pk": "ad_category_pk",
            "fk": "ad_id",
            "parent": Ad,
            "type": "child",
        },
        {
            "model": AdLanguage,
            "csv": "ad_languages_processed.csv",
            "pk": "ad_language_pk",
            "fk": "ad_id",
            "parent": Ad,
            "type": "child",
        },
        {
            "model": AdRate,
            "csv": "ad_rates_processed.csv",
            "pk": "ad_rate_pk",
            "fk": "ad_id",
            "parent": Ad,
            "type": "child",
        },
        {
            "model": AdTag,
            "csv": "ad_tags_processed.csv",
            "pk": "ad_tag_pk",
            "fk": "ad_id",
            "parent": Ad,
            "type": "child",
        },
        {
            "model": UserRole,
            "csv": "user_roles_processed.csv",
            "pk": "user_role_pk",
            "fk": "user_id",
            "parent": User,
            "type": "child",
        },
    ]

    try:
        for item in load_tasks:
            csv_file = BASE_PREPROCESSED_DIR / item["csv"]
            model_class = item["model"]
            item_type = item["type"]

            if csv_file.is_file():
                logger.info(
                    f"--- Processing {item_type}: {model_class.__tablename__} from {csv_file} ---"
                )
                try:
                    df = pd.read_csv(csv_file, low_memory=False)
                    if df.empty:
                        logger.warning(f"CSV '{csv_file}' empty. Skipping.")
                        continue

                    if item_type == "parent":
                        load_parent_table(session, df, model_class, item["pk"])
                    elif item_type == "child":
                        load_child_table(
                            session,
                            df,
                            model_class,
                            item["pk"],
                            item["fk"],
                            item["parent"],
                        )

                except pd.errors.EmptyDataError:
                    logger.warning(f"CSV '{csv_file}' empty. Skipping.")
                except Exception as read_err:
                    logger.error(
                        f"Error reading/processing CSV {csv_file}: {read_err}",
                        exc_info=True,
                    )
            else:
                logger.warning(f"CSV file not found: {csv_file}. Skipping.")

        logger.info("ETL data loading tasks completed.")

    except Exception as e:
        logger.error(f"Unexpected error during ETL data loading: {e}", exc_info=True)
        session.rollback()  # Rollback any partial transaction
    finally:
        logger.info("Closing DB session.")
        session.close()
        logger.info("DB session closed.")


# --- Main Execution ---
if __name__ == "__main__":
    logger.info(f"--- Starting Yoopies ETL Script (Fresh - No Schema Setup) ---")
    run_etl()  # Call the main function
    logger.info("--- Yoopies ETL Script Finished ---")


# --- Optional separate function call if needed ---
def run_etl_pipeline():
    logger.info(
        f"--- [run_etl_pipeline] Starting Yoopies ETL Script (Fresh - No Schema Setup) ---"
    )
    run_etl()
    logger.info(f"--- [run_etl_pipeline] Yoopies ETL Script Finished ---")


# --- Function to create schema separately (Use this ONCE or when needed) ---
def create_database_schema():
    """Creates all tables defined in Base.metadata. Assumes engine is initialized."""
    if not engine:
        logger.error("Engine not init. Cannot create schema.")
        return
    logger.info("Attempting to create database schema via Base.metadata.create_all...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Schema creation complete (or tables/indexes already existed).")
    except Exception as e:
        logger.error(f"Schema creation error: {e}", exc_info=True)
