"""Manage the Debezium connector through the Kafka Connect REST API.

Usage:
    python register_connector.py register            # create the connector and wait until it runs
    python register_connector.py status              # connector and task state
    python register_connector.py pause | resume | restart
    python register_connector.py delete
    python register_connector.py register --name shop-cdc-v2 --slot shop_cdc_v2     # a second, independent connector

The database host inside the Compose network is `postgres`. SOURCE_DB_HOST and SOURCE_DB_PORT override it
when Kafka Connect runs somewhere else.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request

from common import CONNECT_URL, CONNECTOR, LAB


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
    request = urllib.request.Request(
        CONNECT_URL + path, method=method, data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def state(name: str) -> tuple[str | None, list[str]]:
    code, body = call("GET", f"/connectors/{name}/status")
    if code != 200 or not body:
        return None, []
    return body["connector"]["state"], [t["state"] for t in body["tasks"]]


def wait_running(name: str, timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        connector, tasks = state(name)
        if connector == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            return
        if "FAILED" in tasks or connector == "FAILED":
            raise SystemExit(f"{name} failed:\n{json.dumps(call('GET', f'/connectors/{name}/status')[1], indent=2)[:3000]}")
        time.sleep(2)
    raise SystemExit(f"{name} did not reach RUNNING within {timeout} seconds")


def wait_for_connect(timeout: int = 180) -> None:
    """The Connect container takes a while to start: wait until its REST API answers."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if call("GET", "/")[0] == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(3)
    raise SystemExit(f"Kafka Connect did not answer at {CONNECT_URL} within {timeout} seconds")


def register(name: str, slot: str | None) -> None:
    wait_for_connect()
    definition = json.loads((LAB / "connector.json").read_text())
    config = definition["config"]
    config["database.hostname"] = os.environ.get("SOURCE_DB_HOST", config["database.hostname"])
    config["database.port"] = os.environ.get("SOURCE_DB_PORT", config["database.port"])
    if slot:
        config["slot.name"] = slot
    code, body = call("PUT", f"/connectors/{name}/config", config)      # PUT creates or updates: safe to repeat
    if code not in (200, 201):
        raise SystemExit(f"register failed ({code}): {body}")
    wait_running(name)
    print(f"{name}: RUNNING (slot {config['slot.name']})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=["register", "status", "pause", "resume", "restart", "delete"])
    parser.add_argument("--name", default=CONNECTOR)
    parser.add_argument("--slot", help="replication slot name (register only)")
    args = parser.parse_args()

    if args.action == "register":
        register(args.name, args.slot)
    elif args.action == "status":
        print(json.dumps(call("GET", f"/connectors/{args.name}/status")[1], indent=2))
    elif args.action == "delete":
        print(call("DELETE", f"/connectors/{args.name}")[0])
    else:
        path = f"/connectors/{args.name}/" + ("restart?includeTasks=true&onlyFailed=false" if args.action == "restart" else args.action)
        code, body = call("POST" if args.action == "restart" else "PUT", path)
        print(args.action, code, body or "")
        if args.action in ("restart", "resume"):
            wait_running(args.name)


if __name__ == "__main__":
    main()
