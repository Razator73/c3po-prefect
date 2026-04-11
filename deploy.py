import os

from prefect import serve
from prefect.schedules import Schedule

from jobs.garmin_export.flow import garmin_export
from jobs.gsheet_budget.flow import (
    budget_new_month,
    budget_reminder,
    budget_summary,
    fun_funds_email,
    update_budget_db,
)
from jobs.scrape_patreon.flow import scrape_patreon
from jobs.ufa_api.flow import update_ufa_data

_IS_PROD = os.getenv("C3PO_ENV") == "prod"


def _set_schedule(cron: str, tz: str = "America/Denver") -> Schedule | None:
    return Schedule(cron=cron, timezone=tz) if _IS_PROD else None


def main() -> None:
    serve(
        garmin_export.to_deployment(
            name="garmin-daily",
            schedule=_set_schedule("0 9 * * 1-6"),
            parameters={"lookback_days": 1},
        ),
        garmin_export.to_deployment(
            name="garmin-weekly",
            schedule=_set_schedule("0 9 * * 0"),
            parameters={"lookback_days": 8},
        ),
        scrape_patreon.to_deployment(
            name="patreon-twice-daily",
            schedule=_set_schedule("0 11,23 * * *"),
        ),
        update_budget_db.to_deployment(
            name="budget-db-daily",
            schedule=_set_schedule("0 23 * * *"),
        ),
        fun_funds_email.to_deployment(
            name="budget-fun-funds-email",
            schedule=_set_schedule("0 8 2 * *"),
        ),
        budget_new_month.to_deployment(
            name="budget-new-month",
            schedule=_set_schedule("5 5 27 1-11 *"),
        ),
        budget_reminder.to_deployment(
            name="budget-reminder",
            schedule=_set_schedule("5 5 1 * *"),
        ),
        budget_summary.to_deployment(
            name="budget-summary",
            schedule=_set_schedule("5 5 5 * *"),
        ),
        update_ufa_data.to_deployment(
            name="ufa-api-daily",
            schedule=_set_schedule("0 0 * * *"),
        ),
    )


if __name__ == "__main__":
    main()
