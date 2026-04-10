import os
from typing import Any

import razator_utils
from prefect.client.schemas.objects import FlowRun
from prefect.flows import Flow


def discord_failure_hook(flow: Flow[Any, Any], flow_run: FlowRun, state: Any) -> None:
    discord_url = os.environ["DISCORD_ALERT_URL"]
    url_base = "c3po.razator.cc" if os.getenv("C3PO_ENV") == "prod" else "localhost:4200"
    http = "https" if os.getenv("C3PO_ENV") == "prod" else "http"
    flow_run_url = f"{http}://{url_base}/flow-runs/flow-run/{flow_run.id}"
    message = f":x: **{flow.name}** failed\n[Run Logs]({flow_run_url})"
    razator_utils.discord_message(discord_url, message)
