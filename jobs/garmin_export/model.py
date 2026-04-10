# mypy: ignore-errors
import os

from sqlalchemy import (
    BIGINT,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


def convert_speed_to_pace(speed):
    pace = 1 / (speed * 0.00062137)  # get pace in seconds per mile
    pace_minutes = pace // 60
    pace_seconds = pace - pace_minutes * 60
    return f"{pace_minutes:.0f}:{pace_seconds:02.0f}"


class GarminStat(Base):
    __tablename__ = "daily_stats"
    date = Column(Date, primary_key=True)
    day_of_week = Column(String(9), nullable=False)
    wellness_active_calories = Column(Integer, nullable=False)
    wellness_bmr_calories = Column(Integer, nullable=False)
    food_calories_remaining = Column(Integer, nullable=False)
    wellness_total_calories = Column(Integer, nullable=False)
    total_steps = Column(Integer, nullable=False)
    step_goal = Column(Integer, nullable=False)
    wellness_total_distance = Column(Integer, nullable=False)
    wellness_average_steps = Column(Integer, nullable=False)
    common_total_calories = Column(Integer, nullable=False)
    common_active_calories = Column(Integer, nullable=False)
    common_total_distance = Column(Integer, nullable=False)
    wellness_moderate_intensity_minutes = Column(Integer, nullable=False)
    wellness_vigorous_intensity_minutes = Column(Integer, nullable=False)
    wellness_floors_ascended = Column(Integer, nullable=False)
    wellness_floors_descended = Column(Integer, nullable=False)
    wellness_user_intensity_minutes_goal = Column(Integer, nullable=False)
    wellness_user_floors_ascended_goal = Column(Integer, nullable=False)
    wellness_min_heart_rate = Column(Integer, nullable=False)
    wellness_max_heart_rate = Column(Integer, nullable=False)
    wellness_resting_heart_rate = Column(Integer, nullable=False)
    wellness_average_stress = Column(Integer, nullable=False)
    wellness_max_stress = Column(Integer, nullable=False)
    wellness_min_avg_heart_rate = Column(Integer, nullable=False)
    wellness_max_avg_heart_rate = Column(Integer, nullable=False)
    wellness_bodybattery_charged = Column(Integer, nullable=False)
    wellness_bodybattery_drained = Column(Integer, nullable=False)
    wellness_abnormalhr_alerts_count = Column(Integer, nullable=False)


class Activity(Base):
    __tablename__ = "activities"
    activity_id = Column(BIGINT, primary_key=True)
    activity_name = Column(String, nullable=False)
    start_time_local = Column(DateTime, nullable=False)
    start_time_gmt = Column(DateTime, nullable=False)
    activity_type_type_id = Column(BIGINT, nullable=False)
    activity_type_type_key = Column(String, nullable=False)
    activity_type_parent_type_id = Column(BIGINT, nullable=False)
    activity_type_is_hidden = Column(Boolean, nullable=False)
    activity_type_trimmable = Column(Boolean, nullable=False)
    activity_type_restricted = Column(Boolean, nullable=False)
    event_type_type_id = Column(BIGINT, nullable=False)
    event_type_type_key = Column(String, nullable=False)
    event_type_sort_order = Column(BIGINT, nullable=False)
    distance = Column(Float, nullable=False)
    duration = Column(Float, nullable=False)
    moving_duration = Column(Float, nullable=True)
    elevation_gain = Column(Float, nullable=True)
    elevation_loss = Column(Float, nullable=True)
    average_speed = Column(Float, nullable=False)
    max_speed = Column(Float, nullable=True)
    calories = Column(Float, nullable=False)
    average_hr = Column(Float, nullable=True)
    max_hr = Column(Float, nullable=True)
    steps = Column(Float, nullable=True)
    time_zone_id = Column(BIGINT, nullable=False)
    begin_timestamp = Column(BIGINT, nullable=False)
    v_o2_max_value = Column(Float, nullable=True)
    workout_id = Column(Float, nullable=True)
    device_id = Column(Float, nullable=True)
    min_elevation = Column(Float, nullable=True)
    max_elevation = Column(Float, nullable=True)
    location_name = Column(String, nullable=True)
    lap_count = Column(Float, nullable=True)
    calories_consumed = Column(Float, nullable=True)
    min_activity_lap_duration = Column(Float, nullable=True)
    has_splits = Column(Boolean, nullable=True)
    moderate_intensity_minutes = Column(Float, nullable=True)
    vigorous_intensity_minutes = Column(Float, nullable=True)
    pr = Column(Boolean, nullable=False)
    manual_activity = Column(Boolean, nullable=False)
    auto_calc_calories = Column(Boolean, nullable=False)
    elevation_corrected = Column(String, nullable=True)

    @property
    def duration_minutes(self):
        return self.duration / 60 if self.duration else None

    @property
    def distance_miles(self):
        return self.distance * 0.000621371 if self.distance else None

    @property
    def steps_per_mile(self):
        return self.steps / self.distance_miles if self.distance_miles and self.steps else None

    @property
    def steps_per_minute(self):
        return self.steps / self.duration_minutes if self.duration_minutes and self.steps else None

    @property
    def pace(self):
        return convert_speed_to_pace(self.average_speed) if self.average_speed else None


class WeighIn(Base):
    __tablename__ = "weigh_ins"
    weigh_in_id = Column(String, primary_key=True)
    weight_timestamp_utc = Column(DateTime, nullable=False)
    weight_timestamp_mountain = Column(DateTime, nullable=False)
    calendar_date = Column(Date, nullable=False)
    weight_kg = Column(Float, nullable=False)
    weight_lbs = Column(Float, nullable=False)


def init_db():
    engine = create_engine(
        f"postgresql+psycopg2://{os.environ['GARMIN_DATABASE_USER']}:{os.environ['GARMIN_DATABASE_PASSWORD']}"
        f"@{os.environ['DATABASE_HOST']}/{os.environ['GARMIN_DATABASE_DB']}"
    )
    Base.metadata.create_all(engine)

    db_session = sessionmaker(bind=engine)
    return db_session()
