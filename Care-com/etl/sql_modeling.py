# etl_load_postgres_fully_normalized_v3.py

import os
import sys
import json
import logging
from pathlib import Path
import pandas as pd
from typing import List, Optional, Any, Dict
import traceback
from decimal import Decimal, InvalidOperation  # For currency amounts

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
    UniqueConstraint,
    Index,
    Numeric,
    Time,
    Date,
    and_,  # Added and_
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker,
    Session,
    foreign,  # Added foreign
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from config import CARE_COM_PASSWORD

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Database Connection String (replace with your actual credentials)
# Format: postgresql://user:password@host:port/database

DATABASE_URI = f"postgresql://postgres:{CARE_COM_PASSWORD}@localhost:5432/care-com_db"  # Example for PostgreSQL
COUNTRY = "USA"
BASE_PREPROCESSED_DIR = Path("preprocessed_data")
COUNTRY_PREPROCESSED_DIR = BASE_PREPROCESSED_DIR / COUNTRY

# --- Logging Setup ---
LOG_FILE = BASE_PREPROCESSED_DIR / f"etl_load_postgres_{COUNTRY}_fully_normalized.log"
if not LOG_FILE.parent.exists():
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"Log dir: {LOG_FILE.parent}")
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
    SessionLocal = sessionmaker(bind=engine)
    logger.info("DB engine created.")
except Exception as e:
    logger.error(f"DB engine failed: {e}")
    exit(1)


# --- Helper Functions (Unchanged) ---
def parse_json_field(json_string: Optional[str], default_val: Any = None) -> Any:
    if (
        pd.isna(json_string)
        or not isinstance(json_string, str)
        or not json_string.strip()
    ):
        return default_val
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        return default_val


def safe_int(v) -> Optional[int]:
    return (
        None
        if pd.isna(v)
        else (int(v) if not isinstance(v, str) or v.isdigit() else None)
    )


def safe_float(v) -> Optional[float]:
    return (
        None
        if pd.isna(v)
        else (
            float(v)
            if not isinstance(v, str) or v.replace(".", "", 1).isdigit()
            else None
        )
    )


def safe_bool(v) -> Optional[bool]:
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v_lower = v.lower()
        return (
            True
            if v_lower in ["true", "t", "1", "yes", "y"]
            else (False if v_lower in ["false", "f", "0", "no", "n"] else None)
        )
    try:
        return bool(v)
    except (ValueError, TypeError):
        return None


def safe_timestamp(v) -> Optional[pd.Timestamp]:
    ts = pd.to_datetime(v, errors="coerce")
    return ts if pd.notna(ts) else None


def safe_time(v) -> Optional[pd.Timestamp]:
    if pd.isna(v) or not isinstance(v, str):
        return None
    try:
        return pd.to_datetime(v, format="%H:%M:%S", errors="coerce").time()
    except ValueError:
        return None


def safe_decimal(v, default=None) -> Optional[Decimal]:
    if pd.isna(v):
        return default
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return default


# --- SQLAlchemy Models (Fully Normalized - Relationships Fixed) ---


# Profile & Immediate Relations
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
    sign_up_date = Column(TIMESTAMP(timezone=False), nullable=True)
    years_of_experience = Column(Integer, nullable=True)
    cbc_seeker_limit_reached = Column(Boolean, nullable=True)
    cbc_seeker_status = Column(String, nullable=True)
    cbc_has_active_hit = Column(Boolean, nullable=True)
    hired_by_counts_json = Column(JSONB, nullable=True)
    place_info_json = Column(JSONB, nullable=True)
    recurring_availability_json = Column(JSONB, nullable=True)
    continuous_background_check_json = Column(JSONB, nullable=True)
    languages = relationship(
        "ProfileLanguage",
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
    availability_blocks = relationship(
        "ProfileAvailability",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    service_ids = relationship(
        "ProfileServiceId",
        back_populates="profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    nested_childcare = relationship(
        "NestedChildcareProfile",
        back_populates="profile",
        cascade="all, delete-orphan",
        uselist=False,
    )
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
    profile = relationship("Profile", back_populates="education_degrees")
    details_texts = relationship(
        "ProfileEducationDetailText",
        back_populates="degree",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class ProfileEducationDetailText(Base):
    __tablename__ = "profile_education_detail_texts"
    id = Column(Integer, primary_key=True)
    degree_id = Column(
        Integer, ForeignKey("profile_education_degrees.id"), nullable=False
    )
    detail_text = Column(Text, nullable=False)
    degree = relationship("ProfileEducationDegree", back_populates="details_texts")


class ProfileNonPrimaryImage(Base):
    __tablename__ = "profile_non_primary_images"
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    image_url = Column(Text, nullable=False)
    profile = relationship("Profile", back_populates="non_primary_images")
    __table_args__ = (UniqueConstraint("profile_id", "image_url"),)


class ProfileAvailability(Base):
    __tablename__ = "profile_availability"
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, index=True
    )
    day_of_week = Column(String, nullable=False)
    start_time = Column(Time, nullable=True)
    end_time = Column(Time, nullable=True)
    profile = relationship("Profile", back_populates="availability_blocks")
    __table_args__ = (Index("ix_profile_avail_day", "profile_id", "day_of_week"),)


class ProfileServiceId(Base):
    __tablename__ = "profile_service_ids"
    profile_id = Column(String, ForeignKey("profiles.profile_id"), primary_key=True)
    service_id_name = Column(String, primary_key=True)
    profile = relationship("Profile", back_populates="service_ids")


class NestedProfileBase:
    id = Column(Integer, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=False, unique=True
    )
    zip_code = Column(String, nullable=True)
    sub_id = Column(String, nullable=True)
    approval_status = Column(String, nullable=True)
    availability_frequency = Column(String, nullable=True)
    years_of_experience = Column(Integer, nullable=True)
    bio_summary = Column(Text, nullable=True)
    bio_title = Column(String, nullable=True)
    bio_ai_assisted = Column(Boolean, nullable=True)
    payrange_from_amount = Column(Numeric(10, 2), nullable=True)
    payrange_from_currency = Column(String(3), nullable=True)
    payrange_to_amount = Column(Numeric(10, 2), nullable=True)
    payrange_to_currency = Column(String(3), nullable=True)
    recurring_from_amount = Column(Numeric(10, 2), nullable=True)
    recurring_from_currency = Column(String(3), nullable=True)
    recurring_to_amount = Column(Numeric(10, 2), nullable=True)
    recurring_to_currency = Column(String(3), nullable=True)
    service_ids_json = Column(JSONB, nullable=True)


class NestedOtherQuality(Base):
    __tablename__ = "nested_other_qualities"
    id = Column(Integer, primary_key=True)
    nested_profile_id = Column(Integer, nullable=False)
    nested_profile_type = Column(String, nullable=False)
    quality_name = Column(String, nullable=False)
    __table_args__ = (
        Index(
            "ix_nested_other_qual_id_type", "nested_profile_id", "nested_profile_type"
        ),
    )


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
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedCommonProfile.id == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "commonCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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
    childcare_child_staff_ratio = Column(Float, nullable=True)
    childcare_max_age_months = Column(Integer, nullable=True)
    childcare_min_age_months = Column(Integer, nullable=True)
    childcare_number_of_children = Column(Integer, nullable=True)
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
    rates = relationship(
        "NestedChildcareRate",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    age_groups = relationship(
        "NestedChildcareAgeGroup",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedChildcareProfile.id == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "childCareCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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


class NestedChildcareRate(Base):
    __tablename__ = "nested_childcare_rates"
    id = Column(Integer, primary_key=True)
    nested_profile_id = Column(
        Integer, ForeignKey("nested_childcare_profiles.id"), nullable=False, index=True
    )
    num_children = Column(Integer, nullable=True)
    amount = Column(Numeric(10, 2), nullable=True)
    currency = Column(String(3), nullable=True)
    is_default = Column(Boolean, nullable=True)
    nested_profile = relationship("NestedChildcareProfile", back_populates="rates")


class NestedChildcareAgeGroup(Base):
    __tablename__ = "nested_childcare_age_groups"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_childcare_profiles.id"), primary_key=True
    )
    age_group = Column(String, primary_key=True)
    nested_profile = relationship("NestedChildcareProfile", back_populates="age_groups")


# Nested Housekeeping Profile
class NestedHousekeepingProfile(NestedProfileBase, Base):
    __tablename__ = "nested_housekeeping_profiles"
    housekeeping_distance_willing_to_travel = Column(Integer, nullable=True)
    schedule_json = Column(JSONB, nullable=True)
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
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedHousekeepingProfile.id
            == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "houseKeepingCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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
    petcare_number_of_pets_comfortable_with = Column(Integer, nullable=True)
    profile = relationship("Profile", back_populates="nested_petcare")
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
    activity_rates = relationship(
        "NestedPetcareActivityRate",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    service_rates = relationship(
        "NestedPetcareServiceRate",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    species_cared_for = relationship(
        "NestedPetcareSpecies",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedPetcareProfile.id == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "petCareCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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


class NestedPetcareActivityRate(Base):
    __tablename__ = "nested_petcare_activity_rates"
    id = Column(Integer, primary_key=True)
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), nullable=False, index=True
    )
    activity = Column(String)
    amount = Column(Numeric(10, 2), nullable=True)
    currency = Column(String(3))
    unit = Column(String)
    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="activity_rates"
    )


class NestedPetcareServiceRate(Base):
    __tablename__ = "nested_petcare_service_rates"
    id = Column(Integer, primary_key=True)
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), nullable=False, index=True
    )
    subtype = Column(String)
    duration = Column(String)
    amount = Column(Numeric(10, 2), nullable=True)
    currency = Column(String(3))
    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="service_rates"
    )


class NestedPetcareSpecies(Base):
    __tablename__ = "nested_petcare_species"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_petcare_profiles.id"), primary_key=True
    )
    species_name = Column(String, primary_key=True)
    nested_profile = relationship(
        "NestedPetcareProfile", back_populates="species_cared_for"
    )


# Nested Seniorcare Profile
class NestedSeniorcareProfile(NestedProfileBase, Base):
    __tablename__ = "nested_seniorcare_profiles"
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
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedSeniorcareProfile.id == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "seniorCareCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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
    tutoring_other_specific_subject = Column(Text, nullable=True)
    profile = relationship("Profile", back_populates="nested_tutoring")
    qualities = relationship(
        "NestedTutoringQuality",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    supported_services = relationship(
        "NestedTutoringService",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    subjects = relationship(
        "NestedTutoringSubject",
        back_populates="nested_profile",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    other_qualities = relationship(
        "NestedOtherQuality",
        primaryjoin=lambda: and_(
            NestedTutoringProfile.id == foreign(NestedOtherQuality.nested_profile_id),
            NestedOtherQuality.nested_profile_type == "tutoringCaregiverProfile",
        ),
        lazy="selectin",
        viewonly=True,
    )  # Fixed


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


class NestedTutoringSubject(Base):
    __tablename__ = "nested_tutoring_subjects"
    nested_profile_id = Column(
        Integer, ForeignKey("nested_tutoring_profiles.id"), primary_key=True
    )
    subject_name = Column(String, primary_key=True)
    is_specific = Column(Boolean, nullable=False)
    nested_profile = relationship("NestedTutoringProfile", back_populates="subjects")


# Review & Relations
class Review(Base):
    __tablename__ = "reviews"
    review_id = Column(String, primary_key=True)
    profile_id = Column(
        String, ForeignKey("profiles.profile_id"), nullable=True, index=True
    )
    zip_code = Column(String, nullable=True, index=True)
    source_filename = Column(Text, nullable=True)
    care_type = Column(String, nullable=True)
    create_time = Column(TIMESTAMP(timezone=False), nullable=True)
    delete_time = Column(TIMESTAMP(timezone=False), nullable=True)
    description_display_text = Column(Text, nullable=True)
    description_original_text = Column(Text, nullable=True)
    language_code = Column(String, nullable=True)
    original_source = Column(String, nullable=True)
    status = Column(String, nullable=True)
    update_source = Column(String, nullable=True)
    update_time = Column(TIMESTAMP(timezone=False), nullable=True)
    verified_by_care = Column(Boolean, nullable=True)
    reviewee_provider_type = Column(String, nullable=True)
    reviewee_type = Column(String, nullable=True)
    reviewer_image_url = Column(Text, nullable=True)
    reviewer_first_name = Column(String, nullable=True)
    reviewer_last_initial = Column(String(1), nullable=True)
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


# --- Mapping (Unchanged) ---
NESTED_MODEL_MAP = {
    "childCareCaregiverProfile": {
        "model": NestedChildcareProfile,
        "qualities_link": NestedChildcareQuality,
        "services_link": NestedChildcareService,
    },
    "commonCaregiverProfile": {
        "model": NestedCommonProfile,
        "interests_link": NestedCommonInterest,
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
        "qualities_link": NestedTutoringQuality,
        "services_link": NestedTutoringService,
    },
}

# --- ETL Data Loading Functions (Modified for Full Normalization - Unchanged logic, just relying on fixed models) ---
# load_profiles, load_nested_profiles, load_reviews functions are the same as the previous version


def load_profiles(session: Session, df: pd.DataFrame):
    logger.info(f"Starting load {len(df)} main profile records (normalized)...")
    loaded_count = 0
    skipped_count = 0
    for idx, row in df.iterrows():
        profile_id = row.get("profile_id")
        zip_code = row.get("zip_code")
        if pd.isna(profile_id):
            skipped_count += 1
            continue
        try:
            profile = (
                session.query(Profile)
                .filter_by(profile_id=profile_id)
                .options(relationship("*").lazyload("*"))
                .one_or_none()
            )
            is_new = False
            if not profile:
                profile = Profile(profile_id=profile_id)
                session.add(profile)
                is_new = True
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
            profile.sign_up_date = safe_timestamp(row.get("signUpDate"))
            profile.years_of_experience = safe_int(row.get("yearsOfExperience"))
            cbc = parse_json_field(row.get("continuousBackgroundCheck_json"), {})
            cbc_seeker = cbc.get("seeker", {}) if isinstance(cbc, dict) else {}
            profile.cbc_seeker_limit_reached = safe_bool(
                cbc_seeker.get("hasLimitReached")
            )
            profile.cbc_seeker_status = cbc_seeker.get("subscriptionStatus")
            profile.cbc_has_active_hit = safe_bool(cbc.get("hasActiveHit"))
            profile.hired_by_counts_json = parse_json_field(
                row.get("hiredByCounts_json"), {}
            )
            profile.place_info_json = parse_json_field(row.get("placeInfo_json"), None)
            profile.recurring_availability_json = parse_json_field(
                row.get("recurringAvailability_json"), {}
            )

            if not is_new:
                profile.languages.clear()
                profile.badges.clear()
                profile.background_checks.clear()
                profile.education_degrees.clear()
                profile.non_primary_images.clear()
                profile.availability_blocks.clear()
                profile.service_ids.clear()
                session.flush()

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
            degrees_data = parse_json_field(row.get("educationDegrees_json"), [])
            new_degrees = []
            for d in degrees_data:
                if isinstance(d, dict):
                    degree_obj = ProfileEducationDegree(
                        currently_attending=safe_bool(d.get("currentlyAttending")),
                        degree_year=safe_int(d.get("degreeYear")),
                        education_level=d.get("educationLevel"),
                        school_name=d.get("schoolName"),
                    )
                    details = d.get("educationDetailsText", [])
                    degree_obj.details_texts = [
                        ProfileEducationDetailText(detail_text=str(t))
                        for t in details
                        if t and isinstance(details, list)
                    ]
                    new_degrees.append(degree_obj)
            profile.education_degrees = new_degrees
            images = parse_json_field(row.get("nonPrimaryImages_json"), [])
            profile.non_primary_images = [
                ProfileNonPrimaryImage(image_url=str(img_url))
                for img_url in images
                if img_url and isinstance(img_url, str)
            ]
            avail = parse_json_field(row.get("recurringAvailability_json"), {})
            day_list = avail.get("dayList", {}) if isinstance(avail, dict) else {}
            new_availability = []
            if isinstance(day_list, dict):
                for day, schedule in day_list.items():
                    if isinstance(schedule, dict):
                        for block in schedule.get("blocks", []):
                            if isinstance(block, dict):
                                new_availability.append(
                                    ProfileAvailability(
                                        day_of_week=day,
                                        start_time=safe_time(block.get("start")),
                                        end_time=safe_time(block.get("end")),
                                    )
                                )
            profile.availability_blocks = new_availability
            profiles_data = parse_json_field(row.get("profiles_json"), {})
            service_ids_list = (
                profiles_data.get("serviceIds", [])
                if isinstance(profiles_data, dict)
                else []
            )
            if isinstance(service_ids_list, list):
                profile.service_ids = [
                    ProfileServiceId(service_id_name=str(sid))
                    for sid in service_ids_list
                    if sid
                ]

            loaded_count += 1
            # if loaded_count % 1000 == 0: logger.info(f"Processed {loaded_count} profiles...")
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
    if sub_profile_type_name not in NESTED_MODEL_MAP:
        logger.error(f"No model mapping for {sub_profile_type_name}.")
        return
    map_info = NESTED_MODEL_MAP[sub_profile_type_name]
    NestedModel = map_info["model"]
    logger.info(
        f"Starting load {len(df)} nested '{sub_profile_type_name}' into {NestedModel.__tablename__}..."
    )
    loaded_count = 0
    skipped_count = 0
    parent_not_found = 0
    parent_ids = {p_id for p_id, in session.query(Profile.profile_id)}
    # logger.info(f"Fetched {len(parent_ids)} existing parent profile IDs.")

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
        if profile_id not in parent_ids:
            parent_not_found += 1
            continue  # Skip if parent doesn't exist

        try:
            nested = (
                session.query(NestedModel)
                .filter_by(profile_id=profile_id)
                .options(relationship("*").lazyload("*"))
                .one_or_none()
            )
            is_new = False
            if not nested:
                nested = NestedModel(profile_id=profile_id)
                session.add(nested)
                is_new = True

            nested.zip_code = zip_code
            nested.sub_id = row.get(f"{sub_type}_id")
            nested.approval_status = row.get(f"{sub_type}_approvalStatus")
            nested.availability_frequency = row.get(f"{sub_type}_availabilityFrequency")
            nested.years_of_experience = safe_int(
                row.get(f"{sub_type}_yearsOfExperience")
            )
            nested.service_ids_json = parse_json_field(row.get("service_ids_json"), [])
            bio = parse_json_field(row.get(f"{sub_type}_bio_json"), {})
            nested.bio_summary = bio.get("experienceSummary")
            nested.bio_title = bio.get("title")
            nested.bio_ai_assisted = safe_bool(bio.get("aiAssistedBio"))
            pr = parse_json_field(row.get(f"{sub_type}_payRange_json"), {})
            pr_from = pr.get("hourlyRateFrom", {})
            pr_to = pr.get("hourlyRateTo", {})
            nested.payrange_from_amount = safe_decimal(pr_from.get("amount"))
            nested.payrange_from_currency = pr_from.get("currencyCode")
            nested.payrange_to_amount = safe_decimal(pr_to.get("amount"))
            nested.payrange_to_currency = pr_to.get("currencyCode")
            rr = parse_json_field(row.get(f"{sub_type}_recurringRate_json"), {})
            rr_from = rr.get("hourlyRateFrom", {})
            rr_to = rr.get("hourlyRateTo", {})
            nested.recurring_from_amount = safe_decimal(rr_from.get("amount"))
            nested.recurring_from_currency = rr_from.get("currencyCode")
            nested.recurring_to_amount = safe_decimal(rr_to.get("amount"))
            nested.recurring_to_currency = rr_to.get("currencyCode")

            if sub_type == "commonCaregiverProfile":
                nested.repeat_clients_count = safe_int(
                    row.get("commonCaregiverProfile_repeatClientsCount")
                )
            if sub_type == "houseKeepingCaregiverProfile":
                nested.housekeeping_distance_willing_to_travel = safe_int(
                    row.get("houseKeepingCaregiverProfile_distanceWillingToTravel")
                )
                nested.schedule_json = parse_json_field(
                    row.get(f"{sub_type}_schedule_json"), []
                )
            if sub_type == "childCareCaregiverProfile":
                nested.childcare_child_staff_ratio = safe_float(
                    row.get("childCareCaregiverProfile_childStaffRatio")
                )
                nested.childcare_max_age_months = safe_int(
                    row.get("childCareCaregiverProfile_maxAgeMonths")
                )
                nested.childcare_min_age_months = safe_int(
                    row.get("childCareCaregiverProfile_minAgeMonths")
                )
                nested.childcare_number_of_children = safe_int(
                    row.get("childCareCaregiverProfile_numberOfChildren")
                )
            if sub_type == "petCareCaregiverProfile":
                nested.petcare_number_of_pets_comfortable_with = safe_int(
                    row.get("petCareCaregiverProfile_numberOfPetsComfortableWith")
                )
            if sub_type == "tutoringCaregiverProfile":
                nested.tutoring_other_specific_subject = row.get(
                    "tutoringCaregiverProfile_otherSpecificSubject"
                )

            if is_new:
                session.flush()  # Get nested.id

            # Clear and Populate Normalized Collections
            if not is_new:
                if hasattr(nested, "qualities") and nested.qualities:
                    nested.qualities.clear()
                if hasattr(nested, "supported_services") and nested.supported_services:
                    nested.supported_services.clear()
                if (
                    hasattr(nested, "merchandized_job_interests")
                    and nested.merchandized_job_interests
                ):
                    nested.merchandized_job_interests.clear()
                if hasattr(nested, "rates") and nested.rates:
                    nested.rates.clear()
                if hasattr(nested, "age_groups") and nested.age_groups:
                    nested.age_groups.clear()
                if hasattr(nested, "other_qualities") and nested.other_qualities:
                    nested.other_qualities.clear()
                if hasattr(nested, "activity_rates") and nested.activity_rates:
                    nested.activity_rates.clear()
                if hasattr(nested, "service_rates") and nested.service_rates:
                    nested.service_rates.clear()
                if hasattr(nested, "species_cared_for") and nested.species_cared_for:
                    nested.species_cared_for.clear()
                if hasattr(nested, "subjects") and nested.subjects:
                    nested.subjects.clear()
                session.flush()

            if (
                "qualities_link" in map_info
                and hasattr(nested, "qualities")
                and f"{sub_type}_qualities_list" in row
            ):
                qualities = parse_json_field(row.get(f"{sub_type}_qualities_list"), [])
                nested.qualities = [
                    map_info["qualities_link"](quality_name=str(q))
                    for q in qualities
                    if q
                ]
            if (
                "services_link" in map_info
                and hasattr(nested, "supported_services")
                and f"{sub_type}_supportedServices_list" in row
            ):
                services = parse_json_field(
                    row.get(f"{sub_type}_supportedServices_list"), []
                )
                nested.supported_services = [
                    map_info["services_link"](service_name=str(s))
                    for s in services
                    if s
                ]
            if (
                "interests_link" in map_info
                and hasattr(nested, "merchandized_job_interests")
                and f"{sub_type}_merchandizedJobInterests_list" in row
            ):
                interests = parse_json_field(
                    row.get(f"{sub_type}_merchandizedJobInterests_list"), []
                )
                nested.merchandized_job_interests = [
                    map_info["interests_link"](interest_name=str(i))
                    for i in interests
                    if i
                ]
            if (
                hasattr(nested, "other_qualities")
                and f"{sub_type}_otherQualities_json" in row
            ):
                other_quals = parse_json_field(
                    row.get(f"{sub_type}_otherQualities_json"), []
                )
                nested.other_qualities = [
                    NestedOtherQuality(
                        nested_profile_id=nested.id,
                        nested_profile_type=sub_type,
                        quality_name=str(oq),
                    )
                    for oq in other_quals
                    if oq
                ]

            if sub_type == "childCareCaregiverProfile":
                rates = parse_json_field(row.get(f"{sub_type}_rates_json"), [])
                nested.rates = [
                    NestedChildcareRate(
                        num_children=safe_int(r.get("numberOfChildren")),
                        amount=safe_decimal(r.get("hourlyRate", {}).get("amount")),
                        currency=r.get("hourlyRate", {}).get("currencyCode"),
                        is_default=safe_bool(r.get("isDefaulted")),
                    )
                    for r in rates
                    if isinstance(r, dict)
                ]
                ages = parse_json_field(row.get(f"{sub_type}_ageGroups_json"), [])
                nested.age_groups = [
                    NestedChildcareAgeGroup(age_group=str(ag)) for ag in ages if ag
                ]
            elif sub_type == "petCareCaregiverProfile":
                act_rates = parse_json_field(row.get(f"{sub_type}_rates_json"), [])
                nested.activity_rates = [
                    NestedPetcareActivityRate(
                        activity=r.get("activity"),
                        amount=safe_decimal(r.get("activityRate", {}).get("amount")),
                        currency=r.get("activityRate", {}).get("currencyCode"),
                        unit=r.get("activityRateUnit"),
                    )
                    for r in act_rates
                    if isinstance(r, dict)
                ]
                srv_rates = parse_json_field(
                    row.get(f"{sub_type}_serviceRates_json"), []
                )
                nested.service_rates = [
                    NestedPetcareServiceRate(
                        subtype=r.get("subtype"),
                        duration=r.get("duration"),
                        amount=safe_decimal(r.get("rate", {}).get("amount")),
                        currency=r.get("rate", {}).get("currencyCode"),
                    )
                    for r in srv_rates
                    if isinstance(r, dict)
                ]
                species = parse_json_field(row.get(f"{sub_type}_petSpecies_json"), {})
                species_list = (
                    [
                        NestedPetcareSpecies(species_name=k)
                        for k, v in species.items()
                        if v is True and k != "__typename"
                    ]
                    if isinstance(species, dict)
                    else []
                )
                nested.species_cared_for = species_list
            elif sub_type == "tutoringCaregiverProfile":
                gen_subj = parse_json_field(
                    row.get(f"{sub_type}_otherGeneralSubjects_json"), []
                )
                spec_subj = parse_json_field(
                    row.get(f"{sub_type}_specificSubjects_json"), []
                )
                subjects = [
                    NestedTutoringSubject(subject_name=str(s), is_specific=False)
                    for s in gen_subj
                    if s
                ] + [
                    NestedTutoringSubject(subject_name=str(s), is_specific=True)
                    for s in spec_subj
                    if s
                ]
                nested.subjects = subjects

            loaded_count += 1
            # if loaded_count % 1000 == 0: logger.info(f"Processed {loaded_count} nested {sub_type} records...")
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
        f"Nested '{sub_profile_type_name}' load done. Loaded: {loaded_count}, Skipped: {skipped_count}, ParentNotFound: {parent_not_found}"
    )


def load_reviews(session: Session, df: pd.DataFrame):
    """Loads review data and related normalized tables."""
    logger.info(f"Starting load {len(df)} review records (normalized)...")
    loaded_count = 0
    skipped_count = 0
    parent_not_found = 0
    parent_ids = {p_id for p_id, in session.query(Profile.profile_id)}

    for idx, row in df.iterrows():
        review_id = row.get("review_id")
        profile_id = row.get("profile_id")
        zip_code = row.get("zip_code")
        if pd.isna(review_id):
            skipped_count += 1
            continue
        if pd.notna(profile_id) and profile_id not in parent_ids:
            profile_id = None
            parent_not_found += 1
        try:
            review = (
                session.query(Review)
                .filter_by(review_id=review_id)
                .options(relationship("*").lazyload("*"))
                .one_or_none()
            )
            is_new = False
            if not review:
                review = Review(review_id=review_id)
                session.add(review)
                is_new = True
            # Populate simple fields
            review.profile_id = profile_id
            review.zip_code = zip_code
            review.source_filename = row.get("source_filename")
            review.care_type = row.get("careType")
            review.create_time = safe_timestamp(row.get("createTime"))
            review.delete_time = safe_timestamp(row.get("deleteTime"))
            review.description_display_text = row.get("description_displayText")
            review.description_original_text = row.get("description_originalText")
            review.language_code = row.get("languageCode")
            review.original_source = row.get("originalSource")
            review.status = row.get("status")
            review.update_source = row.get("updateSource")
            review.update_time = safe_timestamp(row.get("updateTime"))
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
            if is_new:
                session.flush()
            if not is_new:
                review.ratings.clear()
                review.attributes.clear()
                session.flush()
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
            loaded_count += 1
            # if loaded_count % 1000 == 0: logger.info(f"Processed {loaded_count} reviews...")
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


# --- Main ETL Function (Mostly Unchanged, Relies on Updated Load Functions) ---
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
            profiles_df.drop_duplicates(
                subset=["profile_id"], keep="first", inplace=True
            )
            logger.info(f"Deduplicated main profiles: {len(profiles_df)} remaining.")
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
                    parts[1]
                    if len(parts) > 2
                    and parts[0] == "nested"
                    and parts[-1] == "processed"
                    else None
                )
                if sub_profile_type_name:
                    logger.info(
                        f"Reading nested profiles: {nested_csv} (Type: {sub_profile_type_name})"
                    )
                    nested_df = pd.read_csv(nested_csv, low_memory=False)
                    nested_df.drop_duplicates(
                        subset=["profile_id"], keep="first", inplace=True
                    )
                    logger.info(
                        f"Deduplicated nested {sub_profile_type_name}: {len(nested_df)} remaining."
                    )
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
            reviews_df.drop_duplicates(subset=["review_id"], keep="first", inplace=True)
            logger.info(f"Deduplicated reviews: {len(reviews_df)} remaining.")
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


# --- Main Execution ---
if __name__ == "__main__":
    logger.info(
        f"Starting ETL Script: Load Processed CSVs to PostgreSQL DB for {COUNTRY} (Fully Normalized v2)"
    )
    run_etl()
    logger.info("ETL Script Finished.")
