import os
from typing import Any

import razator_utils
from prefect.client.schemas.objects import FlowRun
from prefect.flows import Flow


def _send_failure_alert(flow: Flow[Any, Any], flow_run: FlowRun) -> None:
    discord_url = os.environ["DISCORD_ALERT_URL"]
    url_base = "c3po.razator.cc" if os.getenv("C3PO_ENV") == "prod" else "localhost:4200"
    http = "https" if os.getenv("C3PO_ENV") == "prod" else "http"
    flow_run_url = f"{http}://{url_base}/flow-runs/flow-run/{flow_run.id}"
    message = f":x: **{flow.name}** failed\n[Run Logs]({flow_run_url})"
    razator_utils.discord_message(discord_url, message)


def discord_failure_hook(flow: Flow[Any, Any], flow_run: FlowRun, state: Any) -> None:
    _send_failure_alert(flow, flow_run)


def consecutive_failure_hook(threshold: int = 5) -> Any:
    """Returns a hook that only alerts after `threshold` consecutive failures."""

    async def _hook(flow: Flow[Any, Any], flow_run: FlowRun, state: Any) -> None:
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.filters import FlowFilter, FlowFilterName
        from prefect.client.schemas.objects import StateType
        from prefect.client.schemas.sorting import FlowRunSort

        async with get_client() as client:
            runs = await client.read_flow_runs(
                flow_filter=FlowFilter(name=FlowFilterName(any_=[flow.name])),
                sort=FlowRunSort.START_TIME_DESC,
                limit=threshold,
            )

        consecutive = 0
        for run in runs:
            if run.state_type == StateType.FAILED:
                consecutive += 1
            else:
                break

        if consecutive >= threshold:
            _send_failure_alert(flow, flow_run)

    return _hook
