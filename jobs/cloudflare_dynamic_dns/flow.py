import json
import os
from typing import Any

import requests
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from hooks import discord_failure_hook


def _get_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['CLOUDFLARE_API_TOKEN']}",
        "Content-Type": "application/json",
    }


@task(cache_policy=NO_CACHE)
def get_public_ip() -> str:
    r = requests.get("https://api.ipify.org/")
    r.raise_for_status()
    return str(r.text)


@task(cache_policy=NO_CACHE)
def get_dns_records(zone_id: str) -> list[dict[str, Any]]:
    r = requests.get(
        f"https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records",
        headers=_get_headers(),
    )
    r.raise_for_status()
    data = r.json()
    assert data["success"], "Error fetching DNS records"
    return list(data["result"])


@task(cache_policy=NO_CACHE)
def update_dns_records(zone_id: str, records: list[dict[str, Any]], ip_address: str) -> None:
    logger = get_run_logger()
    for record in records:
        if record["type"] not in ("A", "AAAA"):
            continue
        if ip_address == record["content"] or "tunnel" in str(record["content"]):
            continue
        payload = {
            "type": record["type"],
            "name": record["name"],
            "content": ip_address,
            "ttl": record["ttl"],
            "proxied": record["proxied"],
        }
        r = requests.put(
            f"https://api.cloudflare.com/client/v4/zones/{zone_id}/dns_records/{record['id']}",
            headers=_get_headers(),
            data=json.dumps(payload),
        )
        r.raise_for_status()
        data = r.json()
        assert data["success"], f"Error updating record {record}"
        logger.info(f"IP updated to {ip_address} for {record['name']}")


@flow(name="cloudflare-dynamic-dns", on_failure=[discord_failure_hook])
def cloudflare_dynamic_dns() -> None:
    zone_id = os.environ["CLOUDFLARE_ZONE_ID"]
    ip_address = get_public_ip()
    records = get_dns_records(zone_id)
    update_dns_records(zone_id, records, ip_address)
