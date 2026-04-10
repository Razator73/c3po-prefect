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
        update_budget_db.to_deployment(
            name="budget-db-daily",
            schedule=Schedule(cron="0 23 * * *", timezone="America/Denver"),
        ),
        fun_funds_email.to_deployment(
            name="budget-fun-funds-email",
            schedule=Schedule(cron="0 8 2 * *", timezone="America/Denver"),
        ),
        budget_new_month.to_deployment(
            name="budget-new-month",
            schedule=Schedule(cron="5 5 27 1-11 *", timezone="America/Denver"),
        ),
        budget_reminder.to_deployment(
            name="budget-reminder",
            schedule=Schedule(cron="5 5 1 * *", timezone="America/Denver"),
        ),
        budget_summary.to_deployment(
            name="budget-summary",
            schedule=Schedule(cron="5 5 5 * *", timezone="America/Denver"),
        ),
    )


if __name__ == "__main__":
    main()
