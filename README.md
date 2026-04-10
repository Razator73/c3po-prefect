# c3po

Personal Python scripts migrated to [Prefect](https://www.prefect.io/) for scheduling, retries, and observability. Self-hosted on a home network VM.

## Jobs

| Job | Schedule | Description |
|---|---|---|
| `cloudflare_dynamic_dns` | every 10 min | Updates Cloudflare DNS record with current public IP |
| `garmin_export` | 9am Mon–Sat / 9am Sun (8-day lookback) | Exports Garmin activity and health data to PostgreSQL |
| `gsheet_budget` | 7 schedules | Syncs budget data from Google Sheets |
| `scrape_patreon` | 11am & 11pm daily | Scrapes Patreon for new posts |
| `ufa_api` | midnight daily | Updates UFA data via API |

## Structure

```
jobs/           # One directory per job (flow.py + supporting modules)
deploy.py       # Registers all deployments with Prefect
hooks.py        # Shared flow hooks (e.g. Discord failure notifications)
ansible/        # Provisions and configures the Prefect VM
infra/          # Terraform for VM provisioning
scripts/        # Dev utilities
```

## Local development

Requires [uv](https://docs.astral.sh/uv/).

```bash
# Install dependencies
uv sync --extra garmin  # add other --extra flags as needed

# Copy and fill in environment variables
cp .env.example .env

# Start Prefect server + register deployments in one command
./scripts/dev.sh
```

The Prefect UI will be available at http://localhost:4200.

## Deployment

Deployments are handled via GitHub Actions on merge to `main`. The workflow connects via WireGuard VPN and runs the Ansible playbook on the control node.

To deploy manually from the control node:

```bash
cd /opt/c3po-prefect
git pull
cd ansible
uv run ansible-playbook playbook.yml
```

## Environment variables

See `ansible/host_vars/c3po-prefect/vars.yml.example` for all required variables. Key ones:

| Variable | Description |
|---|---|
| `C3PO_ENV` | `prod` or `dev` — controls Prefect UI URL in Discord alerts |
| `DISCORD_ALERT_URL` | Discord webhook URL for failure notifications |
| `DATABASE_HOST` | Shared PostgreSQL host |
