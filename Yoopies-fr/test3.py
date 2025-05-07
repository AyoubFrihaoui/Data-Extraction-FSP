# etl_load_postgres_yoopies_normalized_v2.py

from decimal import Decimal, InvalidOperation
import os
import sys
import json
import logging
from pathlib import Path
import pandas as pd
from typing import List, Optional, Any, Dict
import traceback
from datetime import datetime, time  # Import datetime and time explicitly

from sqlalchemy import (
    Numeric,
    create_engine,
    Column,
    Integer,
    BigInteger,  # Use BigInteger for user_id, ad_id based on sample values
    String,
    Float,
    Boolean,
    ForeignKey,
    Text,
    TIMESTAMP,
    Time,
    UniqueConstraint,
    Index,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker,
    Session,
    configure_mappers,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Assuming config.py exists and contains YOOPIES_PASSWORD
# from config import YOOPIES_PASSWORD # Replace with your actual config import

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Database Connection String (replace with your actual credentials)
# Format: postgresql://user:password@host:port/database

# Example: Replace with your actual DB URI and credentials
# Ensure YOOPIES_PASSWORD is securely managed (e.g., from environment variables or a secrets file)
# Example using dummy password for illustration
from config import YOOPIES_FR_PASSWORD  # Ensure this is defined in your config.py

DATABASE_URI = f"postgresql://postgres:{YOOPIES_FR_PASSWORD}@localhost:5432/yoopies-fr"


COUNTRY = "France"  # Or infer from input data
# Adjust BASE_PREPROCESSED_DIR to point to the directory *containing* 'yoopies' folder
# Assuming your directory structure is something like:
# /path/to/your/project/
# ├── preprocessed_data/
# │   └── yoopies/
# │       ├── ad_badges_processed.csv
# │       ├── ...
# └── etl_load_postgres_yoopies_normalized_v2.py
# If 'yoopies' is directly under preprocessed_data:
BASE_PREPROCESSED_DIR = Path("preprocessed_data")
PROCESSED_DATA_DIR = BASE_PREPROCESSED_DIR / "yoopies"  # Point to the 'yoopies' folder

# --- Logging Setup ---
# Log file will be inside the yoopies processed data directory
LOG_FILE = PROCESSED_DATA_DIR / f"etl_load_postgres_yoopies_normalized_v2.log"
if not LOG_FILE.parent.exists():
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"FATAL: Could not create log directory {LOG_FILE.parent}: {e}")
        sys.exit(1)  # Use sys.exit for critical failures
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# --- SQLAlchemy Setup ---
Base = declarative_base()

# --- SQLAlchemy Models (Yoopies Normalized) ---
# These models remain largely the same as they represent the target structure.


# --- Helper Functions (Mostly unchanged) ---
def parse_json_field(
    json_string: Optional[str], default_val: Any = None, dumps: bool = False
) -> Any:
    """Safely parse a JSON string from CSV, return default on error or if None/empty."""
    if (
        pd.isna(json_string)
        or not isinstance(json_string, str)
        or not json_string.strip()
    ):
        return default_val
    try:
        if dumps:
            json_string = json_string.replace("'", '"')
            json_string = json.dumps(json_string)  # Check if it's a valid JSON string
        return json.loads(json_string)
    except json.JSONDecodeError:
        return default_val


def safe_int(value) -> Optional[int]:
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
    """Convert value to Decimal, returning default (None) on failure or if NaN/None."""
    if pd.isna(value):
        return default
    try:
        # Convert to string first for robustness against floats/ints in source
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):  # Catch potential errors
        logger.warning(
            f"Could not convert '{value}' (type: {type(value)}) to Decimal. Returning default."
        )
        return default


def safe_bool(value) -> Optional[bool]:
    if pd.isna(value):
        return None
    if isinstance(value, str):
        if value.lower() in ["true", "t", "1", "yes", "y"]:
            return True
        if value.lower() in ["false", "f", "0", "no", "n"]:
            return False
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None


def safe_timestamp(value) -> Optional[pd.Timestamp]:
    if pd.isna(value):
        return None
        ts = pd.to_datetime(value, errors="coerce")
        return ts if pd.notna(ts) else None


def safe_time(
    value,
) -> Optional[pd.Timestamp]:  # Using pd.Timestamp for parsing flexibility
    """Convert HH:MM:SS string to datetime.time object, return None on failure."""
    if pd.isna(value) or not isinstance(value, str):
        return None
    try:
        # Use pandas to parse, handling potential errors
        # Use a dummy date part as pd.to_datetime needs it, then extract time
        dt_obj = pd.to_datetime(
            f"1900-01-01 {value}", format="%Y-%m-%d %H:%M:%S", errors="coerce"
        )
        # Return the time component if parsing succeeded
        return dt_obj.time() if pd.notna(dt_obj) else None
    except ValueError:
        logger.warning(f"Could not parse time string: {value}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing time '{value}': {e}")
        return None


# User Table
class User(Base):
    __tablename__ = "users"
    user_id = Column(BigInteger, primary_key=True)
    enabled = Column(Boolean, nullable=True)
    base_type = Column(String, nullable=True)
    is_verified = Column(Boolean, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    applicant_has_ae_status = Column(Boolean, nullable=True)
    applicant_age = Column(Integer, nullable=True)
    photos_thumbnail_url = Column(Text, nullable=True)
    photos_large_url = Column(Text, nullable=True)
    photos_extra_large_url = Column(Text, nullable=True)
    grades_count = Column(Integer, nullable=True)
    grades_average = Column(Float, nullable=True)
    visa_expiry_date = Column(TIMESTAMP(timezone=False), nullable=True)
    # Relationships - these will be populated by loading separate CSVs
    roles = relationship(
        "UserRole",
        back_populates="user",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    ads = relationship(
        "Ad",
        back_populates="user",  # This must match the property name in Ad
        lazy="selectin",
    )
    reviews_received = relationship(
        "Review",
        back_populates="reviewed_user",
        lazy="selectin",
        foreign_keys="[Review.reviewed_user_id]",
    )


class UserRole(Base):
    __tablename__ = "user_roles"
    user_id = Column(
        BigInteger, ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    role_name = Column(String, primary_key=True)
    user = relationship("User", back_populates="roles")


# Ad Table (Main entity)
class Ad(Base):
    __tablename__ = "ads"
    ad_id = Column(BigInteger, primary_key=True)
    user_id = Column(
        BigInteger, ForeignKey("users.user_id"), nullable=True, index=True
    )  # Link to User
    # Source info (added during phase 1) - assuming these are in flattened_ads_processed
    postal_code = Column(String(99), nullable=True, index=True)
    source_filename = Column(Text, nullable=True)  # 'filename' column in flattened_ads
    # Core ad details - assuming these are in flattened_ads_processed or ads_processed
    # Will prioritize flattened_ads_processed if it's the richer source
    care_type = Column(String, nullable=True, index=True)
    sub_type = Column(String, nullable=True, index=True)
    ad_title = Column(String, nullable=True)
    ad_content = Column(
        Text, nullable=True
    )  # This might map from bio in flattened_ads service details
    ad_type = Column(String, nullable=True)
    ad_status = Column(String, nullable=True)
    ad_created_at = Column(TIMESTAMP(timezone=False), nullable=True)
    ad_updated_at = Column(TIMESTAMP(timezone=False), nullable=True)
    ad_experience_amount = Column(String, nullable=True)
    slug_title = Column(String, nullable=True)  # From flattened_ads
    slug_city = Column(String, nullable=True)  # From flattened_ads
    # Address details (flattened from address object in flattened_ads)
    address_latitude = Column(Float, nullable=True)
    address_longitude = Column(Float, nullable=True)
    address_city = Column(String, nullable=True)
    address_city_key = Column(String, nullable=True)  # From flattened_ads
    address_country = Column(String, nullable=True)  # From flattened_ads
    address_zip_code = Column(String(99), nullable=True)
    # Simple JSONB fields from flattened_ads
    sitter_parent_ad_type_json = Column(JSONB, nullable=True)
    sitter_childminder_slots_json = Column(JSONB, nullable=True)
    user_threads_connections_json = Column(
        JSONB, nullable=True
    )  # Linked to user, but stored with ad in flattened?

    # Define the relationship to User
    user = relationship(
        "User", back_populates="ads"
    )  # This must match the property name in User

    # Relationships to normalized ad details - these will be populated by loading separate CSVs
    badges = relationship(
        "AdBadge",
        back_populates="ad",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    categories = relationship(
        "AdCategory",
        back_populates="ad",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    languages = relationship(
        "AdLanguage",
        back_populates="ad",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    rates = relationship(
        "AdRate",
        back_populates="ad",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    tags = relationship(
        "AdTag",
        back_populates="ad",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    reviews = relationship(
        "Review", back_populates="ad", lazy="selectin", foreign_keys="[Review.ad_id]"
    )

    # Relationships to service-specific detail tables (One-to-one)
    # Assuming these details are columns within flattened_ads_processed and need parsing there
    childcare_details = relationship(
        "AdChildcareDetails",
        back_populates="ad",
        cascade="all, delete-orphan",
        uselist=False,
        lazy="selectin",
    )
    # Add relationships for other service types if needed based on flattened_ads columns
    # housekeeping_details = relationship(...)
    # petcare_details = relationship(...)
    # seniorcare_details = relationship(...)
    # tutoring_details = relationship(...)


# Ad Related Normalized Tables (Loaded from separate CSVs)
class AdBadge(Base):
    __tablename__ = "ad_badges"
    ad_badge_pk = Column(BigInteger, primary_key=True)  # From CSV sample
    ad_id = Column(
        BigInteger,
        ForeignKey("ads.ad_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    badge_name = Column(String, nullable=True)
    badge_additional_data = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="badges")
    __table_args__ = (Index("ix_ad_badge_name", "badge_name"),)


class AdCategory(Base):
    __tablename__ = "ad_categories"
    ad_category_pk = Column(BigInteger, primary_key=True)  # From CSV sample
    ad_id = Column(
        BigInteger,
        ForeignKey("ads.ad_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    service = Column(String, nullable=True)
    category = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="categories")
    __table_args__ = (Index("ix_ad_category", "service", "category"),)


class AdLanguage(Base):
    __tablename__ = "ad_languages"
    ad_language_pk = Column(BigInteger, primary_key=True)  # From CSV sample
    ad_id = Column(
        BigInteger,
        ForeignKey("ads.ad_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    language_code = Column(String(10), nullable=True)

    ad = relationship("Ad", back_populates="languages")
    __table_args__ = (Index("ix_ad_language_code", "language_code"),)


class AdRate(Base):
    __tablename__ = "ad_rates"
    ad_rate_pk = Column(BigInteger, primary_key=True)  # From CSV sample
    ad_id = Column(
        BigInteger,
        ForeignKey("ads.ad_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    employment_type = Column(String, nullable=True)
    care_type = Column(String, nullable=True)
    rate_type = Column(String, nullable=True)

    rate_amount = Column(Numeric(10, 2), nullable=True)
    rate_time_unit = Column(String, nullable=True)
    rate_commission = Column(Numeric(10, 2), nullable=True)

    ad = relationship("Ad", back_populates="rates")
    __table_args__ = (
        Index(
            "ix_ad_rates_ad_id_type",
            "ad_id",
            "employment_type",
            "care_type",
            "rate_type",
        ),
    )


class AdTag(Base):
    __tablename__ = "ad_tags"
    ad_tag_pk = Column(BigInteger, primary_key=True)  # From CSV sample
    ad_id = Column(
        BigInteger,
        ForeignKey("ads.ad_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tag_type = Column(String, nullable=True)
    tag_value = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="tags")
    __table_args__ = (Index("ix_ad_tag_type_value", "tag_type", "tag_value"),)


# Service-Specific Ad Detail Tables (Linked one-to-one to Ad, fields from flattened_ads)


# Childcare Specific Details (Populated from columns in flattened_ads)
class AdChildcareDetails(Base):
    __tablename__ = "ad_childcare_details"
    ad_id = Column(
        BigInteger, ForeignKey("ads.ad_id", ondelete="CASCADE"), primary_key=True
    )
    childStaffRatio = Column(Float, nullable=True)
    maxAgeMonths = Column(Integer, nullable=True)
    minAgeMonths = Column(Integer, nullable=True)
    numberOfChildren = Column(Integer, nullable=True)
    rates_json = Column(JSONB, nullable=True)
    otherQualities_json = Column(JSONB, nullable=True)
    bio = Column(Text, nullable=True)

    ad = relationship("Ad", back_populates="childcare_details")

    qualities = relationship(
        "AdChildcareQuality",
        back_populates="childcare_details",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "AdChildcareService",
        back_populates="childcare_details",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    age_groups = relationship(
        "AdChildcareAgeGroup",
        back_populates="childcare_details",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class AdChildcareQuality(Base):
    __tablename__ = "ad_childcare_qualities"
    ad_id = Column(
        BigInteger,
        ForeignKey("ad_childcare_details.ad_id", ondelete="CASCADE"),
        primary_key=True,
    )
    quality_name = Column(String, primary_key=True)
    childcare_details = relationship("AdChildcareDetails", back_populates="qualities")


class AdChildcareService(Base):
    __tablename__ = "ad_childcare_services"
    ad_id = Column(
        BigInteger,
        ForeignKey("ad_childcare_details.ad_id", ondelete="CASCADE"),
        primary_key=True,
    )
    service_name = Column(String, primary_key=True)
    childcare_details = relationship(
        "AdChildcareDetails", back_populates="supported_services"
    )


class AdChildcareAgeGroup(Base):
    __tablename__ = "ad_childcare_age_groups"
    ad_id = Column(
        BigInteger,
        ForeignKey("ad_childcare_details.ad_id", ondelete="CASCADE"),
        primary_key=True,
    )
    age_group = Column(String, primary_key=True)
    childcare_details = relationship("AdChildcareDetails", back_populates="age_groups")


# Add models for other service types if columns exist in flattened_ads:
# AdHousekeepingDetails, AdHousekeepingQuality, AdHousekeepingService
# AdPetcareDetails, AdPetcareQuality, AdPetcareService, AdPetcareSpecies, AdPetcareActivityRate, AdPetcareServiceRate
# AdSeniorcareDetails, AdSeniorcareQuality, AdSeniorcareService
# AdTutoringDetails, AdTutoringQuality, AdTutoringService


# Review Tables (Loaded from reviews_processed.csv)
class Review(Base):
    __tablename__ = "reviews"
    review_pk = Column(BigInteger, primary_key=True)
    ad_id = Column(BigInteger, ForeignKey("ads.ad_id"), nullable=True, index=True)
    reviewed_user_id = Column(
        BigInteger, ForeignKey("users.user_id"), nullable=True, index=True
    )

    # Source info (added during phase 1)
    zip_code = Column(String(10), nullable=True)
    source_filename = Column(Text, nullable=True)

    # Review details from reviews_processed.csv sample
    reviewer_firstName = Column(String, nullable=True)
    reviewer_lastName = Column(String, nullable=True)
    review_content = Column(Text, nullable=True)
    review_generalGrade = Column(Integer, nullable=True)
    review_experienceName = Column(String, nullable=True)

    ad = relationship("Ad", back_populates="reviews", foreign_keys=[ad_id])
    reviewed_user = relationship(
        "User", back_populates="reviews_received", foreign_keys=[reviewed_user_id]
    )

    # If your full review data includes structured ratings or attributes, add models and relationships here.
    # Example (commented out):
    # ratings = relationship("ReviewRating", back_populates="review", cascade="all, delete-orphan", lazy="selectin")
    # attributes = relationship("ReviewAttribute", back_populates="review", cascade="all, delete-orphan", lazy="selectin")


# Example normalized review tables (Adapt based on actual Yoopies review data structure if available)
# class ReviewRating(Base):
#     __tablename__ = "review_ratings"
#     review_pk = Column(BigInteger, ForeignKey("reviews.review_pk", ondelete="CASCADE"), primary_key=True)
#     rating_type = Column(String, primary_key=True)
#     rating_value = Column(Integer, nullable=True)
#     review = relationship("Review", back_populates="ratings")

# class ReviewAttribute(Base):
#     __tablename__ = "review_attributes"
#     review_pk = Column(BigInteger, ForeignKey("reviews.review_pk", ondelete="CASCADE"), primary_key=True)
#     attribute_type = Column(String, primary_key=True)
#     attribute_value = Column(Boolean, nullable=True)
#     review = relationship("Review", back_populates="attributes")


# --- Mapping from Service Type to Ad Detail Model & Link Tables ---
AD_DETAIL_MODEL_MAP = {
    "childcare": {
        "model": AdChildcareDetails,
        "qualities_link": AdChildcareQuality,
        "services_link": AdChildcareService,
        "age_groups_link": AdChildcareAgeGroup,
        "prefix": "childCareCaregiverProfile_",  # Prefix used in flattened_ads columns
        "json_lists": {  # Map relationship names to JSON column suffixes in flattened_ads
            "qualities": "qualities_json",
            "supported_services": "supportedServices_json",
            "age_groups": "ageGroups_json",
            # Add others like 'rates' if it's a JSON list here
        },
        "simple_fields": {  # Map model attribute names to simple column suffixes in flattened_ads
            "childStaffRatio": "childStaffRatio",
            "maxAgeMonths": "maxAgeMonths",
            "minAgeMonths": "minAgeMonths",
            "numberOfChildren": "numberOfChildren",
            "bio": "bio_experienceSummary",
            # Add payrange fields if specific to this service
            # "payrange_from_amount": "payRange_hourlyRateFrom_amount", # Example if flattened
            # "payrange_from_currency": "payRange_hourlyRateFrom_currencyCode",
        },
    },
    # Add mappings for other service types found in flattened_ads_processed.csv:
    # "housekeeping": {
    #     "model": AdHousekeepingDetails,
    #     "qualities_link": AdHousekeepingQuality,
    #     "services_link": AdHousekeepingService,
    #     "prefix": "houseKeepingCaregiverProfile_",
    #     "json_lists": { ... },
    #     "simple_fields": { ... },
    # },
    # etc.
}

engine = None
SessionLocal = None
try:
    # Remove echo=True for production/large loads
    engine = create_engine(DATABASE_URI, echo=False)
    # Explicitly configure mappers *before* creating tables or using sessions
    # This should help resolve relationship mapping issues
    configure_mappers()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logger.info("Database engine created successfully.")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    sys.exit(1)


# --- Helper Functions (Keep as before) ---
# Safe conversion and JSON parsing functions remain the same (defined above the models)


# --- ETL Data Loading Functions ---


def load_users(session: Session):
    """Loads user data from users_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "users_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Users CSV not found: {csv_path}. Skipping user loading.")
        return

    logger.info(f"Reading users: {csv_path}")
    try:
        df_users = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Users CSV is empty: {csv_path}. Skipping user loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading users CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df_users)} user records...")
    loaded_count = 0
    skipped_count = 0
    for idx, row in df_users.iterrows():
        user_id = safe_int(row.get("user_id"))
        if user_id is None:
            logger.warning(f"Skip user {idx} missing ID.")
            skipped_count += 1
            continue

        try:
            user = session.query(User).filter_by(user_id=user_id).one_or_none()
            if not user:
                user = User(user_id=user_id)
                session.add(user)
            else:
                # If user exists, clear relationships that will be re-added
                # (User roles are loaded separately below)
                user.roles.clear()  # Clear existing roles if loading roles from a separate file
                # If other relationships were populated directly from this CSV, clear them here too

            # Populate simple fields from users_processed
            user.enabled = safe_bool(row.get("user_enabled"))
            user.base_type = row.get("user_baseType")
            user.is_verified = safe_bool(row.get("user_isVerified"))
            user.first_name = row.get("user_firstName")
            user.last_name = row.get("user_lastName")
            user.applicant_has_ae_status = safe_bool(
                row.get("user_applicant_hasAEStatus")
            )
            user.applicant_age = safe_int(row.get("user_applicant_age"))
            user.grades_count = safe_int(row.get("user_grades_count"))
            user.grades_average = safe_float(row.get("user_grades_average"))

            # Photos and Visa Expiry
            user.photos_thumbnail_url = row.get("user_photos_thumbnail")
            user.photos_large_url = row.get("user_photos_large")
            user.photos_extra_large_url = row.get("user_photos_extra_large")
            # Assuming user_options_json is a column in users_processed or flattened_ads
            user_options_json_str = row.get("user_options_json")
            if pd.notna(user_options_json_str):
                user_options = parse_json_field(user_options_json_str, {})
                if isinstance(user_options, dict):
                    user.visa_expiry_date = safe_timestamp(
                        user_options.get("visaExpiryDate")
                    )

            session.flush()  # Needed to assign PK if it's a new user, in case other tables link before commit
            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error user {user_id}: {e}")
            session.rollback()  # Rollback the current transaction for this record
        except Exception as e:
            logger.error(f"Error user {user_id}: {e}\n{traceback.format_exc()}")
            session.rollback()
            skipped_count += 1
    logger.info(f"Users load done. Loaded: {loaded_count}, Skipped: {skipped_count}")


def load_user_roles(session: Session):
    """Loads user role data from user_roles_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "user_roles_processed.csv"
    if not csv_path.is_file():
        logger.warning(
            f"User Roles CSV not found: {csv_path}. Skipping user role loading."
        )
        return

    logger.info(f"Reading user roles: {csv_path}")
    try:
        df_roles = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(
            f"User Roles CSV is empty: {csv_path}. Skipping user role loading."
        )
        return
    except Exception as e:
        logger.error(
            f"Error reading user roles CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df_roles)} user role records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0  # Count records whose parent User wasn't found

    # Pre-fetch existing user IDs for faster lookup
    user_ids_in_db = {u[0] for u in session.query(User.user_id).all()}
    logger.info(
        f"Pre-fetched {len(user_ids_in_db)} user IDs from DB for role FK checks."
    )

    for idx, row in df_roles.iterrows():
        user_id = safe_int(row.get("user_id"))
        role_name = row.get("role_name")

        if user_id is None or pd.isna(role_name) or not str(role_name).strip():
            logger.warning(f"Skip user role {idx} missing user_id or role_name.")
            skipped_count += 1
            continue

        # Check if parent User exists
        if user_id not in user_ids_in_db:
            logger.warning(
                f"Parent User {user_id} not found for role '{role_name}'. Skipping role."
            )
            parent_not_found_count += 1
            continue

        try:
            # Check if this specific user role already exists (based on composite PK user_id, role_name)
            existing_role = (
                session.query(UserRole)
                .filter_by(user_id=user_id, role_name=str(role_name))
                .one_or_none()
            )

            if not existing_role:
                user_role = UserRole(user_id=user_id, role_name=str(role_name))
                session.add(user_role)
                loaded_count += 1
            # If it exists, do nothing (no updates needed for this simple structure)

        except SQLAlchemyError as e:
            logger.error(
                f"DB Error loading user role (user_id: {user_id}, role: {role_name}): {e}"
            )
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading user role (user_id: {user_id}, role: {role_name}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"User Roles load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_ads(session: Session):
    """
    Loads ad data from flattened_ads_processed.csv, including linking to Users
    and populating service-specific detail tables (AdChildcareDetails etc.)
    by parsing columns/JSON within this file.
    """
    csv_path = PROCESSED_DATA_DIR / "flattened_ads_processed.csv"
    if not csv_path.is_file():
        logger.error(f"Ads CSV not found: {csv_path}. Aborting ad loading.")
        return

    logger.info(f"Reading ads: {csv_path}")
    try:
        df_ads = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ads CSV is empty: {csv_path}. Skipping ad loading.")
        return
    except Exception as e:
        logger.error(f"Error reading ads CSV {csv_path}: {e}\n{traceback.format_exc()}")
        return

    logger.info(f"Starting to load {len(df_ads)} ad records...")
    loaded_count = 0
    skipped_count = 0
    user_not_found_count = 0

    # Pre-fetch existing user IDs for faster lookup
    user_ids_in_db = {u[0] for u in session.query(User.user_id).all()}
    logger.info(f"Pre-fetched {len(user_ids_in_db)} user IDs from DB for ad FK checks.")

    for idx, row in df_ads.iterrows():
        ad_id = safe_int(row.get("ad_id"))
        user_id = safe_int(row.get("user_id"))

        if ad_id is None:
            logger.warning(f"Skip ad {idx} missing ID.")
            skipped_count += 1
            continue

        try:
            ad = session.query(Ad).filter_by(ad_id=ad_id).one_or_none()
            if not ad:
                ad = Ad(ad_id=ad_id)
                session.add(ad)
                # For a new ad, no need to clear relationships here
            else:
                # If ad exists, clear relationships populated from THIS row's data
                # Relationships populated from separate CSVs are NOT cleared here.
                ad.childcare_details = (
                    None  # Clear existing detail record if loading again
                )
                # Add clears for other service details if implemented

            # --- Link to User ---
            # Assigning ad.user = user relies on the relationship being configured correctly
            user = None
            if user_id is not None:
                if user_id in user_ids_in_db:
                    user = (
                        session.query(User).filter_by(user_id=user_id).one_or_none()
                    )  # Need the object for the relationship
                if user is None:
                    logger.warning(
                        f"Parent User {user_id} not found for ad {ad_id}. Setting user_id FK to NULL."
                    )
                    user_not_found_count += 1
            ad.user = user  # Assign the User object or None. SQLAlchemy handles FK column update.

            # --- Populate simple Ad fields from flattened_ads_processed ---
            ad.postal_code = row.get("postal_code")
            ad.source_filename = row.get("filename")
            ad.care_type = row.get("care_type")
            ad.sub_type = row.get("sub_type")
            ad.ad_title = row.get("ad_title")
            ad.ad_content = row.get("ad_content")  # Assuming this maps directly
            ad.ad_type = row.get("ad_type")
            ad.ad_status = row.get("ad_status")
            ad.ad_created_at = safe_timestamp(row.get("ad_createdAt"))
            ad.ad_updated_at = safe_timestamp(row.get("ad_updatedAt"))
            ad.ad_experience_amount = row.get("ad_experienceAmount")
            ad.slug_title = row.get("slug_title")
            ad.slug_city = row.get("slug_city")
            ad.address_latitude = safe_float(row.get("address_latitude"))
            ad.address_longitude = safe_float(row.get("address_longitude"))
            ad.address_city = row.get("address_city")
            ad.address_city_key = row.get("address_cityKey")
            ad.address_country = row.get("address_country")
            ad.address_zip_code = row.get("address_zipCode")

            # JSONB fields from flattened_ads
            ad.sitter_parent_ad_type_json = parse_json_field(
                row.get("sitter_parentAdType_json"), None
            )
            ad.sitter_childminder_slots_json = parse_json_field(
                row.get("sitter_childminderSlots_json"), None
            )
            # user_threads_connections_json is linked to User, but in ad row
            # Can store as JSONB on Ad or User, or ignore if not needed. Storing on Ad for now.
            ad.user_threads_connections_json = parse_json_field(
                row.get("user_threadsConnections_json"), None
            )

            # --- Load Service-Specific Details from flattened_ads columns/JSON ---
            current_care_type = ad.care_type  # Use the care type from the Ad record
            detail_map_info = AD_DETAIL_MODEL_MAP.get(current_care_type)

            if detail_map_info:
                DetailModel = detail_map_info["model"]
                prefix = detail_map_info.get("prefix")
                simple_fields_map = detail_map_info.get("simple_fields", {})
                json_lists_map = detail_map_info.get("json_lists", {})

                if prefix:
                    detail = (
                        session.query(DetailModel).filter_by(ad_id=ad_id).one_or_none()
                    )
                    if not detail:
                        detail = DetailModel(ad_id=ad_id)
                        session.add(detail)

                    # Populate simple fields using the map
                    for model_attr_name, source_col_suffix in simple_fields_map.items():
                        col_name = f"{prefix}{source_col_suffix}"
                        value = row.get(col_name)
                        # Apply safe converters based on attribute name (basic examples)
                        if model_attr_name in ["childStaffRatio", "grades_average"]:
                            value = safe_float(value)
                        elif model_attr_name in [
                            "maxAgeMonths",
                            "minAgeMonths",
                            "numberOfChildren",
                            "grades_count",
                        ]:
                            value = safe_int(value)
                        # Add more specific converters for other types/fields

                        setattr(detail, model_attr_name, value)

                    # Populate JSONB fields specific to the service
                    if hasattr(detail, "rates_json") and f"{prefix}rates_json" in row:
                        detail.rates_json = parse_json_field(
                            row.get(f"{prefix}rates_json"), None
                        )
                    if (
                        hasattr(detail, "otherQualities_json")
                        and f"{prefix}otherQualities_json" in row
                    ):
                        detail.otherQualities_json = parse_json_field(
                            row.get(f"{prefix}otherQualities_json"), None
                        )
                    # Add others as needed

                    # Normalize lists/flags linked to this detail record from JSON columns
                    # Qualities
                    if (
                        "qualities" in json_lists_map
                        and "qualities_link" in detail_map_info
                    ):
                        json_col_name = f"{prefix}{json_lists_map['qualities']}"
                        qualities_data = parse_json_field(row.get(json_col_name), [])
                        if isinstance(qualities_data, list):
                            # Clear existing before adding new ones
                            getattr(detail, "qualities", []).clear()
                            detail.qualities.extend(
                                [
                                    detail_map_info["qualities_link"](
                                        quality_name=str(q)
                                    )
                                    for q in qualities_data
                                    if q
                                ]
                            )
                        else:
                            logger.debug(
                                f"Service detail qualities data for ad {ad_id} (type {current_care_type}) is not a list from column '{json_col_name}': {qualities_data}. Skipping."
                            )

                    # Supported Services
                    if (
                        "supported_services" in json_lists_map
                        and "services_link" in detail_map_info
                    ):
                        json_col_name = (
                            f"{prefix}{json_lists_map['supported_services']}"
                        )
                        services_data = parse_json_field(row.get(json_col_name), [])
                        if isinstance(services_data, list):
                            getattr(detail, "supported_services", []).clear()
                            detail.supported_services.extend(
                                [
                                    detail_map_info["services_link"](
                                        service_name=str(s)
                                    )
                                    for s in services_data
                                    if s
                                ]
                            )
                        else:
                            logger.debug(
                                f"Service detail services data for ad {ad_id} (type {current_care_type}) is not a list from column '{json_col_name}': {services_data}. Skipping."
                            )

                    # Age Groups (Childcare specific example)
                    if (
                        "age_groups" in json_lists_map
                        and "age_groups_link" in detail_map_info
                    ):
                        json_col_name = f"{prefix}{json_lists_map['age_groups']}"
                        age_groups_data = parse_json_field(row.get(json_col_name), [])
                        if isinstance(age_groups_data, list):
                            getattr(detail, "age_groups", []).clear()
                            detail.age_groups.extend(
                                [
                                    detail_map_info["age_groups_link"](
                                        age_group=str(ag)
                                    )
                                    for ag in age_groups_data
                                    if ag
                                ]
                            )
                        else:
                            logger.debug(
                                f"Service detail age groups data for ad {ad_id} (type {current_care_type}) is not a list from column '{json_col_name}': {age_groups_data}. Skipping."
                            )

                    # Add loading logic for other nested lists/objects within service details if they exist as JSON columns in flattened_ads

                else:
                    logger.warning(
                        f"No column prefix defined for care_type '{current_care_type}' in AD_DETAIL_MODEL_MAP for ad {ad_id}. Skipping service-specific details."
                    )

            session.flush()  # Flush to get ad PK if needed for related tables (AdBadge, AdRate etc.) which are loaded *after* ads

            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(
                f"DB Error ad {ad_id} (User: {user_id}): {e}\n{traceback.format_exc()}"
            )
            session.rollback()
            skipped_count += 1
        except Exception as e:
            logger.error(
                f"Error ad {ad_id} (User: {user_id}): {e}\n{traceback.format_exc()}"
            )
            session.rollback()
            skipped_count += 1

    logger.info(
        f"Ads load done. Loaded: {loaded_count}, Skipped: {skipped_count}, Users not found: {user_not_found_count}"
    )


# --- Loading functions for Ad-related normalized tables (from separate CSVs) ---


def load_ad_badges(session: Session):
    """Loads ad badge data from ad_badges_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "ad_badges_processed.csv"
    if not csv_path.is_file():
        logger.warning(
            f"Ad Badges CSV not found: {csv_path}. Skipping ad badge loading."
        )
        return

    logger.info(f"Reading ad badges: {csv_path}")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ad Badges CSV is empty: {csv_path}. Skipping loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading ad badges CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df)} ad badge records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0

    # Pre-fetch existing ad IDs
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    logger.info(f"Pre-fetched {len(ad_ids_in_db)} ad IDs from DB for badge FK checks.")

    for idx, row in df.iterrows():
        pk = safe_int(row.get("ad_badge_pk"))
        ad_id = safe_int(row.get("ad_id"))
        badge_name = row.get("badge_name")
        badge_additional_data = row.get("badge_additionalData")

        if (
            pk is None
            or ad_id is None
            or pd.isna(badge_name)
            or not str(badge_name).strip()
        ):
            logger.warning(f"Skip ad badge {idx} missing PK, ad_id, or badge_name.")
            skipped_count += 1
            continue

        if ad_id not in ad_ids_in_db:
            logger.warning(
                f"Parent Ad {ad_id} not found for badge '{badge_name}'. Skipping badge."
            )
            parent_not_found_count += 1
            continue

        try:
            existing_badge = (
                session.query(AdBadge).filter_by(ad_badge_pk=pk).one_or_none()
            )

            if not existing_badge:
                badge = AdBadge(
                    ad_badge_pk=pk,
                    ad_id=ad_id,
                    badge_name=str(badge_name),
                    badge_additional_data=(
                        str(badge_additional_data)
                        if pd.notna(badge_additional_data)
                        else None
                    ),
                )
                session.add(badge)
                loaded_count += 1
            else:
                existing_badge.ad_id = ad_id
                existing_badge.badge_name = str(badge_name)
                existing_badge.badge_additional_data = (
                    str(badge_additional_data)
                    if pd.notna(badge_additional_data)
                    else None
                )

        except SQLAlchemyError as e:
            logger.error(f"DB Error loading ad badge (PK: {pk}, ad_id: {ad_id}): {e}")
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading ad badge (PK: {pk}, ad_id: {ad_id}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"Ad Badges load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_ad_categories(session: Session):
    """Loads ad category data from ad_categories_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "ad_categories_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Ad Categories CSV not found: {csv_path}. Skipping loading.")
        return
    logger.info(f"Reading ad categories: {csv_path}")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ad Categories CSV is empty: {csv_path}. Skipping loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading ad categories CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df)} ad category records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    logger.info(
        f"Pre-fetched {len(ad_ids_in_db)} ad IDs from DB for category FK checks."
    )

    for idx, row in df.iterrows():
        pk = safe_int(row.get("ad_category_pk"))
        ad_id = safe_int(row.get("ad_id"))
        service = row.get("service")
        category = row.get("category")

        if (
            pk is None
            or ad_id is None
            or pd.isna(service)
            or not str(service).strip()
            or pd.isna(category)
            or not str(category).strip()
        ):
            logger.warning(
                f"Skip ad category {idx} missing PK, ad_id, service, or category."
            )
            skipped_count += 1
            continue
        if ad_id not in ad_ids_in_db:
            logger.warning(
                f"Parent Ad {ad_id} not found for category '{service}/{category}'. Skipping."
            )
            parent_not_found_count += 1
            continue

        try:
            existing = (
                session.query(AdCategory).filter_by(ad_category_pk=pk).one_or_none()
            )
            if not existing:
                cat = AdCategory(
                    ad_category_pk=pk,
                    ad_id=ad_id,
                    service=str(service),
                    category=str(category),
                )
                session.add(cat)
                loaded_count += 1
            else:
                existing.ad_id = ad_id
                existing.service = str(service)
                existing.category = str(category)
        except SQLAlchemyError as e:
            logger.error(
                f"DB Error loading ad category (PK: {pk}, ad_id: {ad_id}): {e}"
            )
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading ad category (PK: {pk}, ad_id: {ad_id}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"Ad Categories load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_ad_languages(session: Session):
    """Loads ad language data from ad_languages_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "ad_languages_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Ad Languages CSV not found: {csv_path}. Skipping loading.")
        return
    logger.info(f"Reading ad languages: {csv_path}")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ad Languages CSV is empty: {csv_path}. Skipping loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading ad languages CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df)} ad language records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    logger.info(
        f"Pre-fetched {len(ad_ids_in_db)} ad IDs from DB for language FK checks."
    )

    for idx, row in df.iterrows():
        pk = safe_int(row.get("ad_language_pk"))
        ad_id = safe_int(row.get("ad_id"))
        language_code = row.get("language_code")

        if (
            pk is None
            or ad_id is None
            or pd.isna(language_code)
            or not str(language_code).strip()
        ):
            logger.warning(
                f"Skip ad language {idx} missing PK, ad_id, or language_code."
            )
            skipped_count += 1
            continue
        if ad_id not in ad_ids_in_db:
            logger.warning(
                f"Parent Ad {ad_id} not found for language '{language_code}'. Skipping."
            )
            parent_not_found_count += 1
            continue

        try:
            existing = (
                session.query(AdLanguage).filter_by(ad_language_pk=pk).one_or_none()
            )
            if not existing:
                lang = AdLanguage(
                    ad_language_pk=pk, ad_id=ad_id, language_code=str(language_code)
                )
                session.add(lang)
                loaded_count += 1
            else:
                existing.ad_id = ad_id
                existing.language_code = str(language_code)
        except SQLAlchemyError as e:
            logger.error(
                f"DB Error loading ad language (PK: {pk}, ad_id: {ad_id}): {e}"
            )
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading ad language (PK: {pk}, ad_id: {ad_id}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"Ad Languages load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_ad_rates(session: Session):
    """Loads ad rate data from ad_rates_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "ad_rates_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Ad Rates CSV not found: {csv_path}. Skipping loading.")
        return
    logger.info(f"Reading ad rates: {csv_path}")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ad Rates CSV is empty: {csv_path}. Skipping loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading ad rates CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df)} ad rate records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    logger.info(f"Pre-fetched {len(ad_ids_in_db)} ad IDs from DB for rate FK checks.")

    for idx, row in df.iterrows():
        pk = safe_int(row.get("ad_rate_pk"))
        ad_id = safe_int(row.get("ad_id"))
        emp_type = row.get("employment_type")
        care_type = row.get("care_type")
        rate_type = row.get("rate_type")
        amount = safe_decimal(row.get("rate_amount"))
        unit = row.get("rate_timeUnit")
        commission = safe_decimal(row.get("rate_commission"))

        if (
            pk is None
            or ad_id is None
            or pd.isna(emp_type)
            or pd.isna(care_type)
            or pd.isna(rate_type)
        ):
            logger.warning(f"Skip ad rate {idx} missing PK, ad_id, or rate type info.")
            skipped_count += 1
            continue
        if ad_id not in ad_ids_in_db:
            logger.warning(
                f"Parent Ad {ad_id} not found for rate '{emp_type}/{care_type}/{rate_type}'. Skipping."
            )
            parent_not_found_count += 1
            continue

        try:
            existing = session.query(AdRate).filter_by(ad_rate_pk=pk).one_or_none()
            if not existing:
                rate = AdRate(
                    ad_rate_pk=pk,
                    ad_id=ad_id,
                    employment_type=str(emp_type),
                    care_type=str(care_type),
                    rate_type=str(rate_type),
                    rate_amount=amount,
                    rate_time_unit=str(unit) if pd.notna(unit) else None,
                    rate_commission=commission,
                )
                session.add(rate)
                loaded_count += 1
            else:
                existing.ad_id = ad_id
                existing.employment_type = str(emp_type)
                existing.care_type = str(care_type)
                existing.rate_type = str(rate_type)
                existing.rate_amount = amount
                existing.rate_time_unit = str(unit) if pd.notna(unit) else None
                existing.rate_commission = commission

        except SQLAlchemyError as e:
            logger.error(f"DB Error loading ad rate (PK: {pk}, ad_id: {ad_id}): {e}")
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading ad rate (PK: {pk}, ad_id: {ad_id}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"Ad Rates load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_ad_tags(session: Session):
    """Loads ad tag data from ad_tags_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "ad_tags_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Ad Tags CSV not found: {csv_path}. Skipping loading.")
        return
    logger.info(f"Reading ad tags: {csv_path}")
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Ad Tags CSV is empty: {csv_path}. Skipping loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading ad tags CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df)} ad tag records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found_count = 0
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    logger.info(f"Pre-fetched {len(ad_ids_in_db)} ad IDs from DB for tag FK checks.")

    for idx, row in df.iterrows():
        pk = safe_int(row.get("ad_tag_pk"))
        ad_id = safe_int(row.get("ad_id"))
        tag_type = row.get("tag_type")
        tag_value = row.get("tag_value")

        if (
            pk is None
            or ad_id is None
            or pd.isna(tag_type)
            or not str(tag_type).strip()
            or pd.isna(tag_value)
            or not str(tag_value).strip()
        ):
            logger.warning(
                f"Skip ad tag {idx} missing PK, ad_id, tag_type, or tag_value."
            )
            skipped_count += 1
            continue
        if ad_id not in ad_ids_in_db:
            logger.warning(
                f"Parent Ad {ad_id} not found for tag '{tag_type}/{tag_value}'. Skipping."
            )
            parent_not_found_count += 1
            continue

        try:
            existing = session.query(AdTag).filter_by(ad_tag_pk=pk).one_or_none()
            if not existing:
                tag = AdTag(
                    ad_tag_pk=pk,
                    ad_id=ad_id,
                    tag_type=str(tag_type),
                    tag_value=str(tag_value),
                )
                session.add(tag)
                loaded_count += 1
            else:
                existing.ad_id = ad_id
                existing.tag_type = str(tag_type)
                existing.tag_value = str(tag_value)
        except SQLAlchemyError as e:
            logger.error(f"DB Error loading ad tag (PK: {pk}, ad_id: {ad_id}): {e}")
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error loading ad tag (PK: {pk}, ad_id: {ad_id}): {e}\n{traceback.format_exc()}"
            )
            skipped_count += 1
            session.rollback()

    logger.info(
        f"Ad Tags load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found_count}"
    )


def load_reviews(session: Session):
    """Loads review data from reviews_processed.csv."""
    csv_path = PROCESSED_DATA_DIR / "reviews_processed.csv"
    if not csv_path.is_file():
        logger.warning(f"Reviews CSV not found: {csv_path}. Skipping review loading.")
        return

    logger.info(f"Reading reviews: {csv_path}")
    try:
        df_reviews = pd.read_csv(csv_path, low_memory=False)
    except pd.errors.EmptyDataError:
        logger.warning(f"Reviews CSV is empty: {csv_path}. Skipping review loading.")
        return
    except Exception as e:
        logger.error(
            f"Error reading reviews CSV {csv_path}: {e}\n{traceback.format_exc()}"
        )
        return

    logger.info(f"Starting to load {len(df_reviews)} review records...")
    loaded_count = 0
    skipped_count = 0
    ad_not_found_count = 0
    user_not_found_count = 0

    # Pre-fetch existing ad and user IDs for faster FK checks
    ad_ids_in_db = {a[0] for a in session.query(Ad.ad_id).all()}
    user_ids_in_db = {u[0] for u in session.query(User.user_id).all()}
    logger.info(
        f"Pre-fetched {len(ad_ids_in_db)} ad IDs and {len(user_ids_in_db)} user IDs from DB for review FK checks."
    )

    for idx, row in df_reviews.iterrows():
        review_pk = safe_int(row.get("review_pk"))
        ad_id = safe_int(row.get("ad_id"))
        reviewed_user_id = safe_int(row.get("reviewed_user_id"))

        if review_pk is None:
            logger.warning(f"Skip review {idx} missing PK.")
            skipped_count += 1
            continue

        try:
            review = session.query(Review).filter_by(review_pk=review_pk).one_or_none()
            if not review:
                review = Review(review_pk=review_pk)
                session.add(review)
                # No need to clear relationships here for a new review
            else:
                # If review exists, clear relationships that might be re-added
                # (If ReviewRating/ReviewAttribute were loaded from JSON and needed clearing)
                pass  # No relationships loaded from JSON in this simple review model

            # --- Link to Ad (FK) ---
            # We just need the ID for the FK; SQLAlchemy handles the relationship linkage
            if ad_id is not None and ad_id not in ad_ids_in_db:
                logger.warning(
                    f"Parent Ad {ad_id} not found for review {review_pk}. Setting ad_id FK to NULL."
                )
                review.ad_id = None  # Explicitly set to None if parent not found
                ad_not_found_count += 1
            else:
                review.ad_id = ad_id  # Assign the ID if parent exists or ad_id is None

            # --- Link to Reviewed User (FK) ---
            if reviewed_user_id is not None and reviewed_user_id not in user_ids_in_db:
                logger.warning(
                    f"Reviewed User {reviewed_user_id} not found for review {review_pk}. Setting reviewed_user_id FK to NULL."
                )
                review.reviewed_user_id = (
                    None  # Explicitly set to None if parent not found
                )
                user_not_found_count += 1
            else:
                review.reviewed_user_id = reviewed_user_id  # Assign the ID if parent exists or user_id is None

            # Populate simple fields from reviews_processed.csv sample
            review.zip_code = row.get("zip_code")
            review.source_filename = row.get("source_filename")
            review.reviewer_firstName = row.get("reviewer_firstName")
            review.reviewer_lastName = row.get("reviewer_lastName")
            review.review_content = row.get("review_content")
            review.review_generalGrade = safe_int(row.get("review_generalGrade"))
            review.review_experienceName = row.get("review_experienceName")

            # If full review data has more fields (like those in Care.com example), map them here.
            # If review ratings/attributes CSVs exist, load them in separate functions like load_ad_badges.

            session.flush()  # Flush to get review PK if needed for its related tables (ratings, attributes)
            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error review {review_pk}: {e}")
            session.rollback()
        except Exception as e:
            logger.error(f"Error review {review_pk}: {e}\n{traceback.format_exc()}")
            session.rollback()
            skipped_count += 1
    logger.info(
        f"Reviews load done. Loaded: {loaded_count}, Skipped: {skipped_count}, Ad Not Found: {ad_not_found_count}, Reviewed User Not Found: {user_not_found_count}"
    )


# --- Main ETL Function ---
def run_etl():
    if not SessionLocal:
        logger.error("DB session not configured.")
        return

    session = SessionLocal()
    logger.info("DB session created.")

    try:
        logger.info("Creating DB tables if needed...")
        # Base.metadata.create_all is called AFTER configure_mappers now in the setup block
        Base.metadata.create_all(bind=engine)  # Ensure tables exist
        logger.info("Table check complete.")

        # --- Loading Order based on FK Dependencies ---
        # 1. Load Users (no FK dependencies)
        # 2. Load Ads (depends on Users)
        # 3. Load User Roles (depends on Users)
        # 4. Load Ad-related tables (Badges, Categories, etc. - depend on Ads)
        # 5. Load Reviews (depends on Ads and Users)

        # Load Users first
        load_users(session)
        session.commit()
        logger.info("Committed Users.")

        # Load Ads second (links to Users, and populates service details within loop)
        load_ads(session)
        session.commit()
        logger.info("Committed Ads and Service Details.")

        # Load User Roles (links to Users)
        load_user_roles(session)
        session.commit()
        logger.info("Committed User Roles.")

        # Load Ad-related normalized tables (link to Ads)
        load_ad_badges(session)
        session.commit()
        logger.info("Committed Ad Badges.")

        load_ad_categories(session)
        session.commit()
        logger.info("Committed Ad Categories.")

        load_ad_languages(session)
        session.commit()
        logger.info("Committed Ad Languages.")

        load_ad_rates(session)
        session.commit()
        logger.info("Committed Ad Rates.")

        load_ad_tags(session)
        session.commit()
        logger.info("Committed Ad Tags.")

        # Load Reviews last (link to Ads and Users)
        load_reviews(session)
        session.commit()
        logger.info("Committed Reviews.")

        # Add calls for loading other normalized tables if they exist (e.g., Review Ratings/Attributes CSVs)

        logger.info("ETL process completed successfully!")

    except SQLAlchemyError as e:
        logger.error(f"DB error during ETL: {e}\n{traceback.format_exc()}")
        session.rollback()
    except Exception as e:
        logger.error(f"Unexpected ETL error: {e}\n{traceback.format_exc()}")
        session.rollback()
    finally:
        logger.info("Closing DB session.")
        session.close()


# --- Main Execution ---
if __name__ == "__main__":
    logger.info(
        f"Starting ETL Script: Load Processed CSVs to PostgreSQL DB for Yoopies {COUNTRY}"
    )
    run_etl()
    logger.info("ETL Script Finished.")


def run_etl_pipeline():
    logger.info(
        f"Starting ETL Script: Load Processed CSVs to PostgreSQL DB for Yoopies {COUNTRY}"
    )
    run_etl()
    logger.info("ETL Script Finished.")
