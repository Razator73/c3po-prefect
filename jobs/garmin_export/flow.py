import datetime as dt
import os

import pytz
import razator_utils
from garminconnect import Garmin
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from jobs.garmin_export.model import Activity, GarminStat, WeighIn, init_db


@task(cache_policy=NO_CACHE)
def get_daily_stats(api: Garmin, start_date: dt.date, end_date: dt.date) -> list[dict[str, object]]:
    logger = get_run_logger()
    start_date = max(start_date, dt.date(2017, 9, 5))
    logger.info(f"Fetching daily stats from {start_date} to {end_date}")

    day_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    column_mapping = {
        "wellness_active_calories": "wellnessActiveKilocalories",
        "wellness_bmr_calories": "bmrKilocalories",
        "food_calories_remaining": "remainingKilocalories",
        "wellness_total_calories": "wellnessKilocalories",
        "total_steps": "totalSteps",
        "step_goal": "dailyStepGoal",
        "wellness_total_distance": "wellnessDistanceMeters",
        "wellness_average_steps": "",
        "common_total_calories": "totalKilocalories",
        "common_active_calories": "activeKilocalories",
        "common_total_distance": "totalDistanceMeters",
        "wellness_moderate_intensity_minutes": "moderateIntensityMinutes",
        "wellness_vigorous_intensity_minutes": "vigorousIntensityMinutes",
        "wellness_floors_ascended": "floorsAscended",
        "wellness_floors_descended": "floorsDescended",
        "wellness_user_intensity_minutes_goal": "intensityMinutesGoal",
        "wellness_user_floors_ascended_goal": "userFloorsAscendedGoal",
        "wellness_min_heart_rate": "minHeartRate",
        "wellness_max_heart_rate": "maxHeartRate",
        "wellness_resting_heart_rate": "restingHeartRate",
        "wellness_average_stress": "averageStressLevel",
        "wellness_max_stress": "maxStressLevel",
        "wellness_min_avg_heart_rate": "minAvgHeartRate",
        "wellness_max_avg_heart_rate": "maxAvgHeartRate",
        "wellness_bodybattery_charged": "bodyBatteryChargedValue",
        "wellness_bodybattery_drained": "bodyBatteryDrainedValue",
        "wellness_abnormalhr_alerts_count": "abnormalHeartRateAlertsCount",
    }

    garmin_data = []
    for i in range((end_date - start_date).days + 1):
        check_day = start_date + dt.timedelta(days=i)
        day_data: dict[str, object] = {
            "date": check_day,
            "day_of_week": day_of_week[check_day.weekday()],
            "wellness_average_steps": 0,
        }
        day_stats = api.get_stats(check_day.isoformat())
        day_data = {
            **day_data,
            **{k: day_stats[v] if day_stats[v] else 0 for k, v in column_mapping.items() if v},
        }
        abnormal_hr_counts = day_stats["abnormalHeartRateAlertsCount"]
        day_data["wellness_abnormalhr_alerts_count"] = (
            abnormal_hr_counts if abnormal_hr_counts else 0
        )
        garmin_data.append(day_data)

    logger.info(f"Fetched {len(garmin_data)} days of stats")
    return garmin_data


@task(cache_policy=NO_CACHE)
def get_garmin_activities(
    api: Garmin, start_date: dt.date, end_date: dt.date
) -> list[dict[str, object]]:
    logger = get_run_logger()
    start_date = max(start_date, dt.date(2013, 9, 1))
    logger.info(f"Fetching activities from {start_date} to {end_date}")
    acts = api.get_activities_by_date(start_date.isoformat(), end_date.isoformat())

    keep_cols = [
        "activityId",
        "activityName",
        "startTimeLocal",
        "startTimeGMT",
        "activityType",
        "eventType",
        "distance",
        "duration",
        "movingDuration",
        "elevationGain",
        "elevationLoss",
        "averageSpeed",
        "maxSpeed",
        "calories",
        "averageHR",
        "maxHR",
        "steps",
        "timeZoneId",
        "beginTimestamp",
        "vO2MaxValue",
        "workoutId",
        "deviceId",
        "minElevation",
        "maxElevation",
        "locationName",
        "lapCount",
        "caloriesConsumed",
        "minActivityLapDuration",
        "hasSplits",
        "moderateIntensityMinutes",
        "vigorousIntensityMinutes",
        "pr",
        "manualActivity",
        "autoCalcCalories",
        "elevationCorrected",
    ]
    time_columns = ["startTimeLocal", "startTimeGMT"]
    flat_activities = []
    while acts:
        flat_act = razator_utils.flatten_dict(
            {k: v for k, v in acts.pop(0).items() if k in keep_cols}
        )
        rename_columns = {col: razator_utils.camel_to_snake(col) for col in flat_act.keys()}
        for col in time_columns:
            flat_act[col] = dt.datetime.fromisoformat(flat_act[col])
        flat_act = {rename_columns.get(k, k): v for k, v in flat_act.items()}
        if "activity_type_sort_order" in flat_act.keys():
            del flat_act["activity_type_sort_order"]
        flat_activities.append(flat_act)

    logger.info(f"Fetched {len(flat_activities)} activities")
    ultimate_acts = [
        a
        for a in flat_activities
        if a["activity_type_type_id"] == 11 and "Frisbee" in a["activity_name"]
    ]
    for ulti_act in ultimate_acts:
        api.set_activity_type(
            activity_id=ulti_act["activity_id"],
            type_id=213,
            type_key="ultimate_disc",
            parent_type_id=206,
        )
        ulti_act["activity_type_type_id"] = 213
        ulti_act["activity_type_type_key"] = "ultimate_disc"
        ulti_act["activity_type_parent_type_id"] = 206
        logger.info(f"Updated activity {ulti_act['activity_id']} to ultimate disc")

    return flat_activities


@task(cache_policy=NO_CACHE)
def get_weigh_ins(api: Garmin, start_date: dt.date, end_date: dt.date) -> list[dict[str, object]]:
    logger = get_run_logger()
    start_date = max(start_date, dt.date(2017, 1, 13))
    logger.info(f"Fetching weigh-ins from {start_date} to {end_date}")
    raw_weigh_ins = api.get_weigh_ins(start_date.isoformat(), end_date.isoformat())
    weigh_ins_list = []
    for weight_day in raw_weigh_ins["dailyWeightSummaries"]:
        weigh_ins_list += weight_day["allWeightMetrics"]

    weigh_ins = []
    for weigh_in in weigh_ins_list:
        if timestamp_gmt := weigh_in["timestampGMT"]:
            timestamp_utc = dt.datetime.utcfromtimestamp(timestamp_gmt / 1000)
        else:
            timestamp_utc = dt.datetime.utcfromtimestamp(weigh_in["date"] / 1000)
        timestamp_utc = pytz.utc.localize(timestamp_utc)
        weigh_ins.append(
            {
                "weigh_in_id": weigh_in["samplePk"],
                "weight_timestamp_utc": timestamp_utc,
                "weight_timestamp_mountain": timestamp_utc.astimezone(
                    pytz.timezone("America/Denver")
                ),
                "calendar_date": dt.date.fromisoformat(weigh_in["calendarDate"]),
                "weight_kg": round(weigh_in["weight"] / 1000, 2),
                "weight_lbs": round(weigh_in["weight"] * 0.00220462, 1),
            }
        )
    logger.info(f"Fetched {len(weigh_ins)} weigh-ins")
    return weigh_ins


@task
def save_to_db(
    daily_stats: list[dict[str, object]],
    activities: list[dict[str, object]],
    weigh_ins: list[dict[str, object]],
) -> None:
    logger = get_run_logger()
    session = init_db()  # type: ignore[no-untyped-call]
    try:
        for day_stat in daily_stats:
            if (row := session.query(GarminStat).filter_by(date=day_stat["date"])).first():
                row.update(day_stat)
            else:
                session.add(GarminStat(**day_stat))
        for activity in activities:
            if (
                row := session.query(Activity).filter_by(activity_id=activity["activity_id"])
            ).first():
                row.update(activity)
            else:
                session.add(Activity(**activity))
        for weigh_in in weigh_ins:
            if (
                row := session.query(WeighIn).filter_by(weigh_in_id=str(weigh_in["weigh_in_id"]))
            ).first():
                row.update(weigh_in)
            else:
                session.add(WeighIn(**weigh_in))
        session.commit()
        logger.info(
            f"Saved {len(daily_stats)} daily stats, "
            f"{len(activities)} activities, "
            f"{len(weigh_ins)} weigh-ins to DB"
        )
    finally:
        session.close()


@flow(name="garmin-export")
def garmin_export(lookback_days: int = 1) -> None:
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=lookback_days)

    api = Garmin()
    api.login(tokenstore=os.path.expanduser("~/.garminconnect"))

    daily_stats = get_daily_stats(api, start_date, end_date)
    activities = get_garmin_activities(api, start_date, end_date)
    weigh_ins = get_weigh_ins(api, start_date, end_date)
    save_to_db(daily_stats, activities, weigh_ins)


if __name__ == "__main__":
    garmin_export()
