from prefect import serve
from prefect.schedules import Schedule

from jobs.garmin_export.flow import garmin_export
from jobs.scrape_patreon.flow import scrape_patreon


def main() -> None:
    serve(
        garmin_export.to_deployment(
            name="garmin-daily",
            schedule=Schedule(cron="0 9 * * 1-6", timezone="America/Denver"),
            parameters={"lookback_days": 1},
        ),
        garmin_export.to_deployment(
            name="garmin-weekly",
            schedule=Schedule(cron="0 9 * * 0", timezone="America/Denver"),
            parameters={"lookback_days": 8},
        ),
        scrape_patreon.to_deployment(
            name="patreon-twice-daily",
            schedule=Schedule(cron="0 11,23 * * *", timezone="America/Denver"),
        ),
    )


if __name__ == "__main__":
    main()
