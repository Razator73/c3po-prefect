# c3po

Monorepo for migrating personal Python scripts to Prefect orchestration.

## What this is

Five standalone scripts that were previously run via crontab, now being migrated to self-hosted Prefect for scheduling, retries, logging, and a UI.

## Key decisions

- **Orchestrator:** Prefect (lightweight, Pythonic, low friction to wrap existing scripts)
- **Structure:** Monorepo — all jobs under `jobs/`
- **Package manager:** uv
- **Pre-commit:** ruff (lint + format), mypy, detect-secrets

## Jobs

| Directory | Source repo | Schedule |
|---|---|---|
| `jobs/cloudflare_dynamic_dns/` | cloudflare_dynamic_dns | `*/10 * * * *` |
| `jobs/garmin_export/` | garmin-export | `0 9 * * 1-6` / `0 9 * * 0` (8-day lookback) |
| `jobs/gsheet_budget/` | gsheet-budget | 7 cron schedules (see Planka card) |
| `jobs/scrape_patreon/` | scrape-patreon | `0 11,23 * * *` |
| `jobs/ufa_api/` | ufa_api | `0 0 * * *` |

## Planka tracking

Board: **Misc.** on the **Personal Projects** project.

Remaining cards (all in Todo):
- `c3po: Prefect infrastructure` — provision VM, stand up self-hosted Prefect server
- `c3po: Migrate cloudflare_dynamic_dns`
- `c3po: Migrate garmin-export`
- `c3po: Migrate gsheet-budget`
- `c3po: Migrate scrape-patreon`
- `c3po: Migrate ufa_api`
- `c3po: CI/CD pipeline` — GitHub Actions: lint → test → deploy to Prefect on merge

## Workflow convention

When starting a card: move it to WIP on Planka.
When finishing a card: mark all tasks complete, mark card complete, move to Done.
