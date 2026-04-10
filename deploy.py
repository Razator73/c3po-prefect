from prefect import serve
from prefect.schedules import Schedule

from jobs.garmin_export.flow import garmin_export


def main() -> None:
    serve(
        garmin_export.to_deployment(
            name="daily",
            schedule=Schedule(cron="0 9 * * 1-6", timezone="America/Denver"),
            parameters={"lookback_days": 1},
        ),
        garmin_export.to_deployment(
            name="weekly",
            schedule=Schedule(cron="0 9 * * 0", timezone="America/Denver"),
            parameters={"lookback_days": 8},
        ),
    )


if __name__ == "__main__":
    main()
