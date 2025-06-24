# etl_load_postgres_normalized.py

from decimal import Decimal, InvalidOperation
import os
import sys
import json
import logging
from pathlib import Path
import pandas as pd
from typing import List, Optional, Any, Dict
import traceback

from sqlalchemy import (
    Numeric,
    create_engine,
    Column,
    Integer,
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
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from config import CARE_COM_PASSWORD

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Database Connection String (replace with your actual credentials)
# Format: postgresql://user:password@host:port/database

DATABASE_URI = f"postgresql://postgres:{CARE_COM_PASSWORD}@localhost:5432/care-com_db3"  # Example for PostgreSQL
COUNTRY = "USA"
BASE_PREPROCESSED_DIR = Path("preprocessed_data")
COUNTRY_PREPROCESSED_DIR = BASE_PREPROCESSED_DIR / COUNTRY

# --- Logging Setup ---
LOG_FILE = BASE_PREPROCESSED_DIR / f"etl_load_postgres_{COUNTRY}_normalized.log"
if not LOG_FILE.parent.exists():
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"Created log directory: {LOG_FILE.parent}")
    except Exception as e:
        print(f"FATAL: Could not create log directory {LOG_FILE.parent}: {e}")
        exit(1)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# --- SQLAlchemy Setup ---
Base = declarative_base()
engine = None
SessionLocal = None
try:
    engine = create_engine(DATABASE_URI, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logger.info("Database engine created successfully.")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    exit(1)


# --- Helper Functions (Mostly unchanged) ---
def parse_json_field(json_string: Optional[str], default_val: Any = None, dumps: bool = False) -> Any:
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
            json_string =  json.dumps(json_string)  # Check if it's a valid JSON string
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


# --- SQLAlchemy Models (Fully Normalized) ---


# --- Main Profile & Related ---
class Profile(Base):
    __tablename__ = "profiles"
    profile_id = Column(String, primary_key=True)
    zip_code = Column(String, nullable=True, index=True)
    source_filename = Column(Text, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    gender = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    email = Column(String, nullable=True)
    primary_service = Column(String, nullable=True, index=True)
    hi_res_image_url = Column(Text, nullable=True)
    image_url = Column(Text, nullable=True)
    address_city = Column(String, nullable=True)
    address_state = Column(String(10), nullable=True)
    address_zip = Column(String(20), nullable=True)
    legacy_id = Column(String, nullable=True)
    is_premium = Column(Boolean, nullable=True)
    distance_from_seeker = Column(Float, nullable=True)
    has_care_check = Column(Boolean, nullable=True)
    has_granted_criminal_bgc_access = Column(Boolean, nullable=True)
    has_granted_mvr_bgc_access = Column(Boolean, nullable=True)
    has_granted_premier_bgc_access = Column(Boolean, nullable=True)
    hired_times = Column(Integer, nullable=True)
    is_favorite = Column(Boolean, nullable=True)
    is_mvr_eligible = Column(Boolean, nullable=True)
    is_vaccinated = Column(Boolean, nullable=True)
    provider_status = Column(String, nullable=True)
    response_rate = Column(Integer, nullable=True)
    response_time = Column(Integer, nullable=True)
    sign_up_date = Column(String, nullable=True)
    years_of_experience = Column(Integer, nullable=True)
    # Store complex objects as JSONB if not fully normalized below
    hired_by_counts_json = Column(JSONB, nullable=True)
    place_info_json = Column(JSONB, nullable=True)
    recurring_availability_json = Column(JSONB, nullable=True)
    continuous_background_check_json = Column(JSONB, nullable=True)
    # Relationships
    offered_services = relationship(
        "ProfileOfferedService",
        back_populates="profile",
        cascade="all, delete-orphan",  # Delete offered services if profile is deleted
        lazy="selectin",
    )
    languages = relationship(
        "ProfileLanguage",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    availability_blocks = relationship(
        "ProfileAvailability",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    badges = relationship(
        "ProfileBadge",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    background_checks = relationship(
        "ProfileBackgroundCheck",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    education_degrees = relationship(
        "ProfileEducationDegree",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    non_primary_images = relationship(
        "ProfileNonPrimaryImage",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    nested_childcare = relationship(
        "NestedChildcareProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )  # One-to-one
    nested_common = relationship(
        "NestedCommonProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
    nested_petcare = relationship(
        "NestedPetcareProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
    nested_seniorcare = relationship(
        "NestedSeniorcareProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
    nested_tutoring = relationship(
        "NestedTutoringProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
    nested_housekeeping = relationship(
        "NestedHousekeepingProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
    reviews = relationship(
        "Review",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ProfileAvailability(Base):
    __tablename__ = "profile_availability"
    id = Column(
        Integer, primary_key=True
    )  # Synthetic primary key for the availability record itself
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    day_of_week = Column(String, nullable=False)  # e.g., 'monday', 'tuesday'
    start_time = Column(Time, nullable=True)  # Store as TIME type
    end_time = Column(Time, nullable=True)  # Store as TIME type

    profile = relationship("Profile", back_populates="availability_blocks")

    # Optional: Index for faster lookup by profile and day
    __table_args__ = (Index("ix_profile_avail_day", "profile_id", "day_of_week"),)


class ProfileOfferedService(Base):
    __tablename__ = "profile_offered_services"
    # Composite primary key ensures a profile doesn't list the same service twice
    profile_id = Column(
        String, ForeignKey("profiles.profile_id", ondelete="CASCADE"), primary_key=True
    )
    service_name = Column(String, primary_key=True)  # e.g., CHILD_CARE, SENIOR_CARE

    profile = relationship(
        "Profile", back_populates="offered_services"
    )  # Correct back_populates

    # Optional index for lookups based on service name
    __table_args__ = (Index("ix_profile_offered_service_name", "service_name"),)


class ProfileLanguage(Base):
    __tablename__ = "profile_languages"
    profile_id = Column(String, ForeignKey("profiles.profile_id"), primary_key=True)
    language = Column(String, primary_key=True)
    profile = relationship("Profile", back_populates="languages")


class ProfileBadge(Base):
    __tablename__ = "profile_badges"
    profile_id = Column(String, ForeignKey("profiles.profile_id"), primary_key=True)
    badge = Column(String, primary_key=True)
    profile = relationship("Profile", back_populates="badges")


class ProfileBackgroundCheck(Base):
    __tablename__ = "profile_background_checks"
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    check_name = Column(String, nullable=True)
    completed_at = Column(TIMESTAMP(timezone=False), nullable=True)
    profile = relationship("Profile", back_populates="background_checks")
    __table_args__ = (UniqueConstraint("profile_id", "check_name", "completed_at"),)


class ProfileEducationDegree(Base):
    __tablename__ = "profile_education_degrees"
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    currently_attending = Column(Boolean, nullable=True)
    degree_year = Column(Integer, nullable=True)
    education_level = Column(String, nullable=True)
    school_name = Column(Text, nullable=True)
    details_text_json = Column(JSONB, nullable=True)
    profile = relationship("Profile", back_populates="education_degrees")


class ProfileNonPrimaryImage(Base):
    __tablename__ = "profile_non_primary_images"
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    image_url = Column(Text, nullable=False)
    profile = relationship("Profile", back_populates="non_primary_images")
    __table_args__ = (UniqueConstraint("profile_id", "image_url"),)


# --- Nested Profile Base (Common Fields) ---
class NestedProfileBase:  # Not a table, just for common fields mixin (optional)
    id = Column(Integer, primary_key=True)  # Synthetic PK for each nested table
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, unique=True
    )  # One-to-one link
    zip_code = Column(String, nullable=True)
    sub_id = Column(String, nullable=True)  # ID from sub-profile source JSON
    approval_status = Column(String, nullable=True)
    availability_frequency = Column(String, nullable=True)
    years_of_experience = Column(Integer, nullable=True)
    # Common complex fields stored as JSONB (can be normalized further if needed)
    bio = Column(String, nullable=True)
    payrange_from_amount =  Column(String, nullable=True)  #Column(Numeric(10, 2), nullable=True)
    payrange_from_currency = Column(String, nullable=True)
    payrange_to_amount = Column(String, nullable=True)
    payrange_to_currency = Column(String, nullable=True)
    # pay_range_json = Column(JSONB, nullable=True)
    recurring_rate_json = Column(JSONB, nullable=True)
    service_ids_json = Column(JSONB, nullable=True)  # List of parent service IDs


# --- Specific Nested Profile Tables & Related ---


# Nested Common Profile
class NestedCommonProfile(NestedProfileBase, Base):
    __tablename__ = "nested_common_profiles"
    repeat_clients_count = Column(Integer, nullable=True)
    profile = relationship("Profile", back_populates="nested_common")
    merchandized_job_interests = relationship(
        "NestedCommonInterest",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NestedCommonInterest(Base):
    __tablename__ = "nested_common_interests"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_common_profiles.id"), primary_key=True
    )
    interest_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedCommonProfile", back_populates="merchandized_job_interests"
    )


# Nested Childcare Profile
class NestedChildcareProfile(NestedProfileBase, Base):
    __tablename__ = "nested_childcare_profiles"
    child_staff_ratio = Column(Float, nullable=True)
    max_age_months = Column(Integer, nullable=True)
    min_age_months = Column(Integer, nullable=True)
    number_of_children = Column(Integer, nullable=True)
    rates_json = Column(JSONB, nullable=True)
    age_groups_json = Column(JSONB, nullable=True)
    other_qualities_json = Column(
        JSONB, nullable=True
    )  # Store less structured lists as JSONB
    profile = relationship("Profile", back_populates="nested_childcare")
    qualities = relationship(
        "NestedChildcareQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "NestedChildcareService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # ADDED Relationship:
    age_groups = relationship(
        "NestedChildcareAgeGroup",
        back_populates="nested_profile",
        cascade="all, delete-orphan",  # Delete age groups if profile is deleted
        lazy="selectin",
    )


class NestedChildcareAgeGroup(Base):
    __tablename__ = "nested_childcare_age_groups"
    # Composite primary key: ensures a profile doesn't have the same age group twice
    nested_profile_id = Column(
        Integer, ForeignKey("nested_childcare_profiles.id"), primary_key=True
    )
    age_group = Column(String, primary_key=True)  # e.g., NEWBORN, TODDLER

    nested_profile = relationship("NestedChildcareProfile", back_populates="age_groups")


class NestedChildcareQuality(Base):
    __tablename__ = "nested_childcare_qualities"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_childcare_profiles.id"), primary_key=True
    )
    quality_name = Column(String, primary_key=True)
    nested_profile = relationship("NestedChildcareProfile", back_populates="qualities")


class NestedChildcareService(Base):
    __tablename__ = "nested_childcare_services"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_childcare_profiles.id"), primary_key=True
    )
    service_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedChildcareProfile", back_populates="supported_services"
    )


# Nested Housekeeping Profile
class NestedHousekeepingProfile(NestedProfileBase, Base):
    __tablename__ = "nested_housekeeping_profiles"
    distance_willing_to_travel = Column(Integer, nullable=True)
    schedule_json = Column(JSONB, nullable=True)
    other_qualities_json = Column(JSONB, nullable=True)
    profile = relationship("Profile", back_populates="nested_housekeeping")
    qualities = relationship(
        "NestedHousekeepingQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "NestedHousekeepingService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NestedHousekeepingQuality(Base):
    __tablename__ = "nested_housekeeping_qualities"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_housekeeping_profiles.id"), primary_key=True
    )
    quality_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedHousekeepingProfile", back_populates="qualities"
    )


class NestedHousekeepingService(Base):
    __tablename__ = "nested_housekeeping_services"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_housekeeping_profiles.id"), primary_key=True
    )
    service_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedHousekeepingProfile", back_populates="supported_services"
    )


# Nested Petcare Profile
class NestedPetcareProfile(NestedProfileBase, Base):
    __tablename__ = "nested_petcare_profiles"
    number_of_pets_comfortable_with = Column(Integer, nullable=True)
    rates_json = Column(JSONB, nullable=True)
    other_qualities_json = Column(JSONB, nullable=True)
    # pet_species_json = Column(JSONB, nullable=True) # Delete species
    profile = relationship("Profile", back_populates="nested_petcare")
    service_rates_normalized = relationship(
        "NestedPetcareServiceRate",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="NestedPetcareServiceRate.subtype",  # Optional ordering
    )
    species_cared_for = relationship(
        "NestedPetcareSpecies",
        back_populates="nested_profile",
        cascade="all, delete-orphan",  # Delete species if petcare profile is deleted
        lazy="selectin",
    )
    activity_rates = relationship(
        "NestedPetcareActivityRate",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    qualities = relationship(
        "NestedPetcareQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "NestedPetcareService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NestedPetcareSpecies(Base):
    __tablename__ = "nested_petcare_species"
    # Composite primary key: one petcare profile can care for a species only once
    nested_profile_id = Column(
        Integer,
        ForeignKey("nested_petcare_profiles.id", ondelete="CASCADE"),
        primary_key=True,
    )
    species_name = Column(String, primary_key=True)  # e.g., caresForDogs, caresForCats

    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="species_cared_for"
    )

    # Optional: Add an index if you frequently query species across profiles
    # __table_args__ = (Index('ix_petcare_species_name', 'species_name'), )


class NestedPetcareActivityRate(Base):
    __tablename__ = "nested_petcare_activity_rates"
    id = Column(Integer, primary_key=True)
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), nullable=False, index=True
    )
    activity = Column(
        String, nullable=True
    )  # e.g., Sitting, Boarding, 30-minute dog walk
    amount = Column(Numeric(10, 2), nullable=True)
    currency = Column(String(3), nullable=True)
    unit = Column(String, nullable=True)  # e.g., night, day, walk
    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="activity_rates"
    )
    __table_args__ = (
        UniqueConstraint(
            "nested_profile_id", "activity", "unit", name="uq_petcare_activity_rate"
        ),
    )


class NestedPetcareServiceRate(Base):
    __tablename__ = "nested_petcare_service_rates"
    id = Column(Integer, primary_key=True)  # Synthetic PK for the rate record
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), nullable=False, index=True
    )
    subtype = Column(String, nullable=True)  # e.g., SITTING, BOARDING, WALKING, DAYCARE
    duration = Column(
        String, nullable=True
    )  # e.g., OVER_NIGHT, PER_DAY, PER_30_MIN, PER_HOUR
    amount = Column(Numeric(10, 2), nullable=True)
    currency = Column(String(3), nullable=True)

    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="service_rates_normalized"
    )  # Use a distinct name

    # Optional: Constraint to ensure combination is unique per profile if needed
    # __table_args__ = (UniqueConstraint('nested_profile_id', 'subtype', 'duration', name='uq_petcare_service_rate'), )


class NestedPetcareQuality(Base):
    __tablename__ = "nested_petcare_qualities"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), primary_key=True
    )
    quality_name = Column(String, primary_key=True)
    nested_profile = relationship("NestedPetcareProfile", back_populates="qualities")


class NestedPetcareService(Base):
    __tablename__ = "nested_petcare_services"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), primary_key=True
    )
    service_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="supported_services"
    )


# Nested Seniorcare Profile
class NestedSeniorcareProfile(NestedProfileBase, Base):
    __tablename__ = "nested_seniorcare_profiles"
    other_qualities_json = Column(JSONB, nullable=True)
    profile = relationship("Profile", back_populates="nested_seniorcare")
    qualities = relationship(
        "NestedSeniorcareQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "NestedSeniorcareService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NestedSeniorcareQuality(Base):
    __tablename__ = "nested_seniorcare_qualities"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_seniorcare_profiles.id"), primary_key=True
    )
    quality_name = Column(String, primary_key=True)
    nested_profile = relationship("NestedSeniorcareProfile", back_populates="qualities")


class NestedSeniorcareService(Base):
    __tablename__ = "nested_seniorcare_services"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_seniorcare_profiles.id"), primary_key=True
    )
    service_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedSeniorcareProfile", back_populates="supported_services"
    )


# Nested Tutoring Profile
class NestedTutoringProfile(NestedProfileBase, Base):
    __tablename__ = "nested_tutoring_profiles"
    other_specific_subject = Column(Text, nullable=True)
    other_general_subjects_json = Column(JSONB, nullable=True)
    other_qualities_json = Column(JSONB, nullable=True)
    specific_subjects_json = Column(JSONB, nullable=True)
    profile = relationship("Profile", back_populates="nested_tutoring")
    qualities = relationship(
        "NestedTutoringQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )  # Assuming qualities is dict of bools
    supported_services = relationship(
        "NestedTutoringService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class NestedTutoringQuality(Base):
    __tablename__ = "nested_tutoring_qualities"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_tutoring_profiles.id"), primary_key=True
    )
    quality_name = Column(String, primary_key=True)
    nested_profile = relationship("NestedTutoringProfile", back_populates="qualities")


class NestedTutoringService(Base):
    __tablename__ = "nested_tutoring_services"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_tutoring_profiles.id"), primary_key=True
    )
    service_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedTutoringProfile", back_populates="supported_services"
    )


# --- Review & Related ---
class Review(Base):
    __tablename__ = "reviews"
    review_id = Column(String, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=True, index=True
    )
    zip_code = Column(String, nullable=True, index=True)
    source_filename = Column(Text, nullable=True)
    care_type = Column(String, nullable=True)
    create_time = Column(String, nullable=True)
    delete_time = Column(String, nullable=True)
    description_display_text = Column(Text, nullable=True)
    description_original_text = Column(Text, nullable=True)
    language_code = Column(String, nullable=True)
    original_source = Column(String, nullable=True)
    status = Column(String, nullable=True)
    update_source = Column(String, nullable=True)
    update_time = Column(String, nullable=True)
    verified_by_care = Column(Boolean, nullable=True)
    reviewee_provider_type = Column(String, nullable=True)
    reviewee_type = Column(String, nullable=True)
    reviewer_image_url = Column(Text, nullable=True)
    reviewer_first_name = Column(String, nullable=True)
    reviewer_last_initial = Column(String, nullable=True)
    reviewer_source = Column(String, nullable=True)
    reviewer_type = Column(String, nullable=True)
    retort_json = Column(JSONB, nullable=True)
    profile = relationship("Profile", back_populates="reviews")
    ratings = relationship(
        "ReviewRating",
        back_populates="review",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    attributes = relationship(
        "ReviewAttribute",
        back_populates="review",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ReviewRating(Base):
    __tablename__ = "review_ratings"
    review_id = Column(String, ForeignKey("reviews.review_id"), primary_key=True)
    rating_type = Column(String, primary_key=True)
    rating_value = Column(Integer, nullable=True)
    review = relationship("Review", back_populates="ratings")


class ReviewAttribute(Base):
    __tablename__ = "review_attributes"
    review_id = Column(String, ForeignKey("reviews.review_id"), primary_key=True)
    attribute_type = Column(String, primary_key=True)
    attribute_value = Column(Boolean, nullable=True)
    review = relationship("Review", back_populates="attributes")


# --- Mapping from sub_profile_type string to Model Class & Link Tables ---
# This is crucial for dynamically loading into the correct table
NESTED_MODEL_MAP = {
    "childCareCaregiverProfile": {
        "model": NestedChildcareProfile,
        "qualities_link": NestedChildcareQuality,
        "services_link": NestedChildcareService,
    },
    "commonCaregiverProfile": {
        "model": NestedCommonProfile,
        "interests_link": NestedCommonInterest,  # Special name for merchandizedJobInterests
    },
    "houseKeepingCaregiverProfile": {
        "model": NestedHousekeepingProfile,
        "qualities_link": NestedHousekeepingQuality,
        "services_link": NestedHousekeepingService,
    },
    "petCareCaregiverProfile": {
        "model": NestedPetcareProfile,
        "qualities_link": NestedPetcareQuality,
        "services_link": NestedPetcareService,
    },
    "seniorCareCaregiverProfile": {
        "model": NestedSeniorcareProfile,
        "qualities_link": NestedSeniorcareQuality,
        "services_link": NestedSeniorcareService,
    },
    "tutoringCaregiverProfile": {
        "model": NestedTutoringProfile,
        "qualities_link": NestedTutoringQuality,  # Assumes qualities dict is flags
        "services_link": NestedTutoringService,
    },
}


# --- ETL Data Loading Functions ---


def load_profiles(session: Session, df: pd.DataFrame):
    """Loads main profile data and related normalized tables."""
    logger.info(f"Starting to load {len(df)} main profile records...")
    loaded_count = 0
    skipped_count = 0
    for idx, row in df.iterrows():
        profile_id = row.get("profile_id")
        zip_code = row.get("zip_code")
        if pd.isna(profile_id):
            logger.warning(f"Skip profile {idx} missing ID.")
            skipped_count += 1
            continue
        try:
            profile = (
                session.query(Profile).filter_by(profile_id=profile_id).one_or_none()
            )
            if not profile:
                profile = Profile(profile_id=profile_id)
                session.add(profile)
            # Populate simple fields
            profile.zip_code = zip_code
            profile.source_filename = row.get("source_filename")
            profile.first_name = row.get("member_firstName")
            profile.last_name = row.get("member_lastName")
            profile.gender = row.get("member_gender")
            profile.display_name = row.get("member_displayName")
            profile.email = row.get("member_email")
            profile.primary_service = row.get("member_primaryService")
            profile.hi_res_image_url = row.get("member_hiResImageURL")
            profile.image_url = row.get("member_imageURL")
            profile.address_city = row.get("member_address_city")
            profile.address_state = row.get("member_address_state")
            profile.address_zip = row.get("member_address_zip")
            profile.legacy_id = row.get("member_legacyId")
            profile.is_premium = safe_bool(row.get("member_isPremium"))
            profile.distance_from_seeker = safe_float(
                row.get("distanceFromSeekerInMiles")
            )
            profile.has_care_check = safe_bool(row.get("hasCareCheck"))
            profile.has_granted_criminal_bgc_access = safe_bool(
                row.get("hasGrantedCriminalBGCAccess")
            )
            profile.has_granted_mvr_bgc_access = safe_bool(
                row.get("hasGrantedMvrBGCAccess")
            )
            profile.has_granted_premier_bgc_access = safe_bool(
                row.get("hasGrantedPremierBGCAccess")
            )
            profile.hired_times = safe_int(row.get("hiredTimes"))
            profile.is_favorite = safe_bool(row.get("isFavorite"))
            profile.is_mvr_eligible = safe_bool(row.get("isMVREligible"))
            profile.is_vaccinated = safe_bool(row.get("isVaccinated"))
            profile.provider_status = row.get("providerStatus")
            profile.response_rate = safe_int(row.get("responseRate"))
            profile.response_time = safe_int(row.get("responseTime"))
            profile.sign_up_date = row.get("signUpDate")
            profile.years_of_experience = safe_int(row.get("yearsOfExperience"))
            # JSONB fields
            profile.hired_by_counts_json = parse_json_field(
                row.get("hiredByCounts_json"), {}
            )
            profile.place_info_json = parse_json_field(row.get("placeInfo_json"), None)
            profile.continuous_background_check_json = parse_json_field(
                row.get("continuousBackgroundCheck_json"), {}
            )

            # Handle Relationships
            languages = parse_json_field(row.get("member_languages_json"), [])
            profile.languages = [
                ProfileLanguage(language=str(lang)) for lang in languages if lang
            ]
            badges = parse_json_field(row.get("badges_json"), [])
            profile.badges = [
                ProfileBadge(badge=str(badge)) for badge in badges if badge
            ]
            checks = parse_json_field(row.get("backgroundChecks_json"), [])
            profile.background_checks = [
                ProfileBackgroundCheck(
                    check_name=c.get("backgroundCheckName"),
                    completed_at=safe_timestamp(c.get("whenCompleted")),
                )
                for c in checks
                if isinstance(c, dict)
            ]
            degrees = parse_json_field(row.get("educationDegrees_json"), [])
            profile.education_degrees = [
                ProfileEducationDegree(
                    currently_attending=safe_bool(d.get("currentlyAttending")),
                    degree_year=safe_int(d.get("degreeYear")),
                    education_level=d.get("educationLevel"),
                    school_name=d.get("schoolName"),
                    details_text_json=d.get("educationDetailsText"),
                )
                for d in degrees
                if isinstance(d, dict)
            ]
            images = parse_json_field(row.get("nonPrimaryImages_json"), [])
            profile.non_primary_images = [
                ProfileNonPrimaryImage(image_url=str(img_url))
                for img_url in images
                if img_url and isinstance(img_url, str)
            ]
            # Availability blocks
            new_availability = []
            avail = parse_json_field(
                row.get("recurringAvailability_json"), {}
            )  # Parse the JSON
            day_list = avail.get("dayList", {}) if isinstance(avail, dict) else {}
            if isinstance(day_list, dict):
                for day, schedule in day_list.items():  # 'monday', 'tuesday', etc.
                    # Ensure day name is valid if needed, or store as string
                    valid_days = {
                        "monday",
                        "tuesday",
                        "wednesday",
                        "thursday",
                        "friday",
                        "saturday",
                        "sunday",
                    }
                    if day == "__typename":  # Skip typename field
                        continue
                    elif day not in valid_days:
                        logger.warning(
                            f"Invalid day '{day}' in availability for profile {profile_id}. Skipping."
                        )
                        continue

                    if isinstance(schedule, dict):
                        blocks = schedule.get("blocks", [])
                        if isinstance(blocks, list):
                            for block in blocks:
                                if isinstance(block, dict):
                                    start_t = safe_time(block.get("start"))
                                    end_t = safe_time(block.get("end"))
                                    # Only add if both start and end times are valid
                                    if start_t is not None and end_t is not None:
                                        new_availability.append(
                                            ProfileAvailability(
                                                day_of_week=day,
                                                start_time=start_t,
                                                end_time=end_t,
                                            )
                                        )
                                    else:
                                        logger.warning(
                                            f"Invalid time block found for {day}, profile {profile_id}: start='{block.get('start')}', end='{block.get('end')}'. Skipping block."
                                        )
                        else:
                            logger.warning(
                                f"Availability 'blocks' is not a list for {day}, profile {profile_id}. Skipping day."
                            )
                    else:
                        logger.warning(
                            f"Availability schedule for '{day}' is not a dict for profile {profile_id}. Skipping day."
                        )

            # Assign the new list to the relationship
            profile.availability_blocks = new_availability

            # --- >>> NEW: Normalize Offered Services <<< ---
            new_offered_services = []
            profiles_data = parse_json_field(
                row.get("profiles_json"), {}
            )  # Parse the profiles object
            service_ids_list = (
                profiles_data.get("serviceIds", [])
                if isinstance(profiles_data, dict)
                else []
            )
            if isinstance(service_ids_list, list):
                for service_name in service_ids_list:
                    if (
                        isinstance(service_name, str) and service_name
                    ):  # Ensure it's a non-empty string
                        new_offered_services.append(
                            ProfileOfferedService(service_name=service_name)
                        )

            # Assign the new list to the relationship
            profile.offered_services = new_offered_services
            # --- <<< END: Normalize Offered Services >>> ---

            session.flush()
            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error profile {profile_id}: {e}")
            session.rollback()
        except Exception as e:
            logger.error(f"Error profile {profile_id}: {e}\n{traceback.format_exc()}")
            session.rollback()
            skipped_count += 1
    logger.info(f"Profiles load done. Loaded: {loaded_count}, Skipped: {skipped_count}")


def load_nested_profiles(
    session: Session, df: pd.DataFrame, sub_profile_type_name: str
):
    """Loads data for a specific nested profile type into its dedicated table."""
    if sub_profile_type_name not in NESTED_MODEL_MAP:
        logger.error(
            f"No model mapping found for nested type: {sub_profile_type_name}. Skipping."
        )
        return
    map_info = NESTED_MODEL_MAP[sub_profile_type_name]
    NestedModel = map_info["model"]
    logger.info(
        f"Starting load {len(df)} nested '{sub_profile_type_name}' records into {NestedModel.__tablename__}..."
    )
    loaded_count = 0
    skipped_count = 0
    parent_not_found = 0
    pk_count = 0

    for idx, row in df.iterrows():
        profile_id = row.get("profile_id")
        sub_type = row.get("sub_profile_type")
        zip_code = row.get("zip_code")
        if (
            pd.isna(profile_id)
            or pd.isna(sub_type)
            or sub_type != sub_profile_type_name
        ):
            skipped_count += 1
            continue
        if (
            session.query(Profile.profile_id).filter_by(profile_id=profile_id).scalar()
            is None
        ):
            parent_not_found += 1
            continue

        try:
            # Find existing or create new nested record
            nested = (
                session.query(NestedModel)
                .filter_by(profile_id=profile_id)
                .one_or_none()
            )  # Unique constraint helps here
            if not nested:
                nested = NestedModel(profile_id=profile_id)
                session.add(nested)

            # --- Populate Explicit/Simple Fields ---
            nested.zip_code = zip_code
            nested.sub_id = row.get(f"{sub_profile_type_name}_id")
            nested.approval_status = row.get(f"{sub_profile_type_name}_approvalStatus")
            nested.availability_frequency = row.get(
                f"{sub_profile_type_name}_availabilityFrequency"
            )
            nested.years_of_experience = safe_int(
                row.get(f"{sub_profile_type_name}_yearsOfExperience")
            )
            # nested.service_ids_json = parse_json_field(row.get("service_ids_json"), [])

            # --- Populate JSONB Fields ---
            nested.bio = row.get(f"{sub_profile_type_name}_bio_experienceSummary")
            
            # --- PayRange Handling ---
            if sub_profile_type_name != "commonCaregiverProfile":
                # pr = parse_json_field(row.get(f"{sub_type}_payRange_json"), {}); # Parse payRange JSON string
                pr_from = json.loads(parse_json_field(row.get(f"{sub_profile_type_name}_payRange_hourlyRateFrom"), {}, dumps=True))
                pr_to = json.loads(parse_json_field(row.get(f"{sub_profile_type_name}_payRange_hourlyRateTo"), {}, dumps=True))

                # Assign to specific DB columns, converting amount with safe_decimal
                # if loaded_count  > 100 and loaded_count < 150:
                #     logger.info(f"PayRange from: {pr_from.get('amount')}, to: {pr_to}")
                #     logger.info(f"{sub_profile_type_name}")
                #     logger.info(f"{row.get('childCareCaregiverProfile_payRange_hourlyRateFrom')}")
                #     logger.info(parse_json_field(row.get(f"{sub_profile_type_name}_payRange_hourlyRateFrom")) )
                nested.payrange_from_amount = pr_from["amount"]
                nested.payrange_from_currency = pr_from["currencyCode"]
                nested.payrange_to_amount = pr_from["amount"]
                nested.payrange_to_currency = pr_to["currencyCode"]
            nested.recurring_rate_json = parse_json_field(
                row.get(f"{sub_profile_type_name}_recurringRate_json"), {}
            )
            # Type-specific JSON fields
            if hasattr(nested, "rates_json"):
                nested.rates_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_rates_json"), []
                )
            # Ages
            ages = parse_json_field(
                row.get(f"{sub_type}_ageGroups_json"), []
            )  # Parse the source JSON list
            if isinstance(ages, list) and hasattr(nested, "age_groups"):
                nested.age_groups = [
                    NestedChildcareAgeGroup(age_group=str(ag)) for ag in ages if ag
                ]  # Assign new objects
            if hasattr(nested, "other_qualities_json"):
                nested.other_qualities_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_otherQualities_json"), []
                )
            if hasattr(nested, "schedule_json"):
                nested.schedule_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_schedule_json"), []
                )
            if hasattr(nested, "service_rates_json"):
                nested.service_rates_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_serviceRates_json"), []
                )
            if hasattr(nested, "pet_species_json"):
                nested.pet_species_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_petSpecies_json"), {}
                )
            if hasattr(nested, "other_general_subjects_json"):
                nested.other_general_subjects_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_otherGeneralSubjects_json"), []
                )
            if hasattr(nested, "specific_subjects_json"):
                nested.specific_subjects_json = parse_json_field(
                    row.get(f"{sub_profile_type_name}_specificSubjects_json"), []
                )

            # --- Populate simple fields specific to certain types ---
            if sub_profile_type_name == "commonCaregiverProfile":
                nested.repeat_clients_count = safe_int(
                    row.get("commonCaregiverProfile_repeatClientsCount")
                )
            if sub_profile_type_name == "houseKeepingCaregiverProfile":
                nested.distance_willing_to_travel = safe_int(
                    row.get("houseKeepingCaregiverProfile_distanceWillingToTravel")
                )
            if sub_profile_type_name == "childCareCaregiverProfile":
                nested.child_staff_ratio = safe_float(
                    row.get("childCareCaregiverProfile_childStaffRatio")
                )
                nested.max_age_months = safe_int(
                    row.get("childCareCaregiverProfile_maxAgeMonths")
                )
                nested.min_age_months = safe_int(
                    row.get("childCareCaregiverProfile_minAgeMonths")
                )
                nested.number_of_children = safe_int(
                    row.get("childCareCaregiverProfile_numberOfChildren")
                )
            if sub_profile_type_name == "petCareCaregiverProfile":
                # add normalized tables
                # >>> Normalize Petcare Activity Rates (from rates_json) <<<
                if hasattr(nested, "activity_rates"):  # Check relationship exists

                    act_rates_data = parse_json_field(
                        row.get(f"{sub_type}_rates_json"), []
                    )
                    new_activity_rates = []
                    if isinstance(act_rates_data, list):
                        for r in act_rates_data:
                            if isinstance(r, dict):
                                rate_info = r.get("activityRate", {})
                                new_activity_rates.append(
                                    NestedPetcareActivityRate(
                                        activity=r.get("activity"),
                                        amount=safe_decimal(rate_info.get("amount")),
                                        currency=rate_info.get("currencyCode"),
                                        unit=r.get("activityRateUnit"),
                                    )
                                )
                    nested.activity_rates = new_activity_rates

                # >>> Normalize Petcare Service Rates (from serviceRates_json) <<<
                if hasattr(
                    nested, "service_rates_normalized"
                ):  # Check relationship exists

                    srv_rates_data = parse_json_field(
                        row.get(f"{sub_type}_serviceRates_json"), []
                    )
                    new_service_rates = []
                    if isinstance(srv_rates_data, list):
                        for r in srv_rates_data:
                            if isinstance(r, dict):
                                rate_info = r.get("rate", {})
                                new_service_rates.append(
                                    NestedPetcareServiceRate(
                                        subtype=r.get("subtype"),
                                        duration=r.get("duration"),
                                        amount=safe_decimal(rate_info.get("amount")),
                                        currency=rate_info.get("currencyCode"),
                                    )
                                )
                    nested.service_rates_normalized = new_service_rates

                # >>> Normalize Petcare Species (from petSpecies_json) <<<
                if hasattr(nested, 'species_cared_for'):
                    species = parse_json_field(row.get(f"{sub_type}_petSpecies_json"),{})
                    species_list = []
                    if isinstance(species, dict):
                         species_list = [NestedPetcareSpecies(species_name=k) for k,v in species.items() if v is True and k != "__typename"]
                    nested.species_cared_for = species_list
                # end add

                # nested.petcare_number_of_pets_comfortable_with = safe_int(
                #     row.get("petCareCaregiverProfile_numberOfPetsComfortableWith")
                # )
            if sub_profile_type_name == "tutoringCaregiverProfile":
                nested.tutoring_other_specific_subject = row.get(
                    "tutoringCaregiverProfile_otherSpecificSubject"
                )

            # Flush to get nested.id if it's newly created
            session.flush()
            pk_count += 1

            # --- Normalize *_list columns (flags) ---
            # Qualities
            if (
                "qualities_link" in map_info
                and f"{sub_profile_type_name}_qualities_flags_json" in row
            ):
                qualities = parse_json_field(
                    row.get(f"{sub_profile_type_name}_qualities_flags_json"), []
                )
                if isinstance(qualities, list):
                    nested.qualities = [
                        map_info["qualities_link"](quality_name=str(q))
                        for q in qualities
                        if q
                    ]
            # Services
            if (
                "services_link" in map_info
                and f"{sub_profile_type_name}_supportedServices_flags_json" in row
            ):
                services = parse_json_field(
                    row.get(f"{sub_profile_type_name}_supportedServices_flags_json"), []
                )
                if isinstance(services, list):
                    nested.supported_services = [
                        map_info["services_link"](service_name=str(s))
                        for s in services
                        if s
                    ]
            # Interests (Common profile specific)
            if (
                "interests_link" in map_info
                and f"{sub_profile_type_name}_merchandizedJobInterests_flags_json"
                in row
            ):
                interests = parse_json_field(
                    row.get(
                        f"{sub_profile_type_name}_merchandizedJobInterests_flags_json"
                    ),
                    [],
                )
                if isinstance(interests, list):
                    nested.merchandized_job_interests = [
                        map_info["interests_link"](interest_name=str(i))
                        for i in interests
                        if i
                    ]

            session.flush()
            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error nested {sub_type} profile {profile_id}: {e}")
            session.rollback()
        except Exception as e:
            logger.error(
                f"Error nested {sub_type} profile {profile_id}: {e}\n{traceback.format_exc()}"
            )
            session.rollback()
            skipped_count += 1
    logger.info(
        f"Nested '{sub_profile_type_name}' load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found}, PKs assigned: {pk_count}"
    )


def load_reviews(session: Session, df: pd.DataFrame):
    """Loads review data and related normalized tables."""
    logger.info(f"Starting load {len(df)} review records...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found = 0
    for idx, row in df.iterrows():
        review_id = row.get("review_id")
        profile_id = row.get("profile_id")
        zip_code = row.get("zip_code")
        if pd.isna(review_id):
            logger.warning(f"Skip review {idx} missing ID.")
            skipped_count += 1
            continue
        if (
            pd.notna(profile_id)
            and session.query(Profile.profile_id)
            .filter_by(profile_id=profile_id)
            .scalar()
            is None
        ):
            logger.warning(
                f"Parent {profile_id} not found review {review_id}. FK set NULL."
            )
            profile_id = None
            parent_not_found += 1
        try:
            review = session.query(Review).filter_by(review_id=review_id).one_or_none()
            if not review:
                review = Review(review_id=review_id)
                session.add(review)
            # Populate simple fields
            review.profile_id = profile_id
            review.zip_code = zip_code
            review.source_filename = row.get("source_filename")
            review.care_type = row.get("careType")
            review.create_time = row.get("createTime")
            review.delete_time = row.get("deleteTime")
            review.description_display_text = row.get("description_displayText")
            review.description_original_text = row.get("description_originalText")
            review.language_code = row.get("languageCode")
            review.original_source = row.get("originalSource")
            review.status = row.get("status")
            review.update_source = row.get("updateSource")
            review.update_time = row.get("updateTime")
            review.verified_by_care = safe_bool(row.get("verifiedByCare"))
            review.reviewee_provider_type = row.get("reviewee_providerType")
            review.reviewee_type = row.get("reviewee_type")
            review.reviewer_image_url = row.get("reviewer_imageURL")
            review.reviewer_first_name = row.get("reviewer_firstName")
            review.reviewer_last_initial = row.get("reviewer_lastInitial")
            review.reviewer_source = row.get("reviewer_source")
            review.reviewer_type = row.get("reviewer_type")
            review.retort_json = parse_json_field(row.get("retort_json"), {})
            # Handle Relationships
            ratings = parse_json_field(row.get("ratings_json"), [])
            review.ratings = [
                ReviewRating(
                    rating_type=r.get("type"), rating_value=safe_int(r.get("value"))
                )
                for r in ratings
                if isinstance(r, dict) and r.get("type")
            ]
            attributes = parse_json_field(row.get("attributes_json"), [])
            review.attributes = [
                ReviewAttribute(
                    attribute_type=a.get("type"),
                    attribute_value=safe_bool(a.get("truthy")),
                )
                for a in attributes
                if isinstance(a, dict) and a.get("type")
            ]

            session.flush()
            loaded_count += 1
        except SQLAlchemyError as e:
            logger.error(f"DB Error review {review_id}: {e}")
            session.rollback()
        except Exception as e:
            logger.error(f"Error review {review_id}: {e}\n{traceback.format_exc()}")
            session.rollback()
            skipped_count += 1
    logger.info(
        f"Reviews load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found}"
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
        Base.metadata.create_all(bind=engine)
        logger.info("Table check complete.")
        # Load Main Profiles first
        profiles_csv = COUNTRY_PREPROCESSED_DIR / "main_profiles_processed.csv"
        if profiles_csv.is_file():
            logger.info(f"Reading main profiles: {profiles_csv}")
            profiles_df = pd.read_csv(profiles_csv, low_memory=False)
            load_profiles(session, profiles_df)
            session.commit()
            logger.info("Committed main profiles.")
        else:
            logger.error(f"Main profiles CSV not found: {profiles_csv}. Aborting.")
            return

        # Load Nested Profiles
        nested_files = list(COUNTRY_PREPROCESSED_DIR.glob("nested_*_processed.csv"))
        if nested_files:
            logger.info(f"Found {len(nested_files)} nested profile CSV files.")
            for nested_csv in nested_files:
                parts = nested_csv.stem.split("_")
                sub_profile_type_name = (
                    parts[1] if len(parts) > 1 and parts[0] == "nested" else None
                )
                if sub_profile_type_name:
                    logger.info(
                        f"Reading nested profiles: {nested_csv} (Type: {sub_profile_type_name})"
                    )
                    nested_df = pd.read_csv(nested_csv, low_memory=False)
                    load_nested_profiles(session, nested_df, sub_profile_type_name)
                    session.commit()
                    logger.info(f"Committed nested profiles: {sub_profile_type_name}")
                else:
                    logger.warning(
                        f"Cannot determine type from filename: {nested_csv}. Skipping."
                    )
        else:
            logger.warning("No nested profile CSV files found.")

        # Load Reviews
        reviews_csv = COUNTRY_PREPROCESSED_DIR / "reviews_processed.csv"
        if reviews_csv.is_file():
            logger.info(f"Reading reviews: {reviews_csv}")
            reviews_df = pd.read_csv(reviews_csv, low_memory=False)
            load_reviews(session, reviews_df)
            session.commit()
            logger.info("Committed reviews.")
        else:
            logger.warning(f"Reviews CSV not found: {reviews_csv}.")
        logger.info("ETL process completed successfully!")
    except SQLAlchemyError as e:
        logger.error(f"DB error during ETL: {e}\n{traceback.format_exc()}")
        session.rollback()
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        session.rollback()
    except Exception as e:
        logger.error(f"Unexpected ETL error: {e}\n{traceback.format_exc()}")
        session.rollback()
    finally:
        logger.info("Closing DB session.")
        session.close()


def print_proposed_data_model():
    """Prints the proposed relational data model (Fully Normalized)."""
    # This is a textual representation, the actual models are defined above
    model = """
    Proposed Relational Data Model (Normalized):
    ---------------------------------------------
    NOTE: This describes the target schema with normalization applied.

    1. profiles (Main Caregiver Info)
       - profile_id (PK), zip_code, source_filename, first_name, last_name, gender, ..., years_of_experience, ...
       - JSONB Columns: hired_by_counts_json, place_info_json, recurring_availability_json, continuous_background_check_json

    2. profile_languages (Normalized)
       - profile_id (PK, FK), language (PK)

    3. profile_badges (Normalized)
       - profile_id (PK, FK), badge (PK)

    4. profile_background_checks (Normalized)
       - id (PK), profile_id (FK), check_name, completed_at

    5. profile_education_degrees (Normalized)
       - id (PK), profile_id (FK), currently_attending, degree_year, education_level, school_name, details_text_json

    6. profile_non_primary_images (Normalized)
       - id (PK), profile_id (FK), image_url

    --- Separate Nested Profile Tables (Examples) ---

    7. nested_common_profiles
       - id (PK), profile_id (FK, Unique), zip_code, sub_id, repeat_clients_count, service_ids_json
    8. nested_common_interests (Normalized from common_..._list)
       - nested_profile_id (PK, FK -> nested_common_profiles.id), interest_name (PK)

    9. nested_childcare_profiles
       - id (PK), profile_id (FK, Unique), zip_code, sub_id, approval_status, availability_frequency, years_of_experience,
         child_staff_ratio, max_age_months, min_age_months, number_of_children,
         bio, pay_range_json, recurring_rate_json, rates_json, age_groups_json, other_qualities_json, service_ids_json
    10. nested_childcare_qualities (Normalized from childCare..._qualities_list)
        - nested_profile_id (PK, FK -> nested_childcare_profiles.id), quality_name (PK)
    11. nested_childcare_services (Normalized from childCare..._supportedServices_list)
        - nested_profile_id (PK, FK -> nested_childcare_profiles.id), service_name (PK)

    12. nested_housekeeping_profiles
        - id (PK), profile_id (FK, Unique), zip_code, sub_id, approval_status, availability_frequency, years_of_experience,
          distance_willing_to_travel, bio, pay_range_json, recurring_rate_json, schedule_json, other_qualities_json, service_ids_json
    13. nested_housekeeping_qualities (...)
    14. nested_housekeeping_services (...)

    15. nested_petcare_profiles (...)
    16. nested_petcare_qualities (...)
    17. nested_petcare_services (...)

    18. nested_seniorcare_profiles (...)
    19. nested_seniorcare_qualities (...)
    20. nested_seniorcare_services (...)

    21. nested_tutoring_profiles (...)
    22. nested_tutoring_qualities (...)
    23. nested_tutoring_services (...)

    --- Review Tables ---

    24. reviews
        - review_id (PK), profile_id (FK), zip_code, source_filename, care_type, create_time, ..., retort_json
    25. review_ratings (Normalized from ratings_json)
        - review_id (PK, FK), rating_type (PK), rating_value
    26. review_attributes (Normalized from attributes_json)
        - review_id (PK, FK), attribute_type (PK), attribute_value
    """
    print(model)


# --- Main Execution ---
if __name__ == "__main__":
    logger.info(
        f"Starting ETL Script: Load Processed CSVs to PostgreSQL DB for {COUNTRY}"
    )
    run_etl()
    # Optionally print the model description at the end
    # print_proposed_data_model()
    logger.info("ETL Script Finished.")
