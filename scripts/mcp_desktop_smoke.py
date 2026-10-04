#!/usr/bin/env python3
"""Verify a running desktop MCP endpoint, without changing research data."""

import argparse
import json
import os
import urllib.error
import urllib.request


def request(url, payload, *, token=None, extra_headers=None):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "SkyBackcraft-Desktop-E2E/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    headers.update(extra_headers or {})
    req = urllib.request.Request(url, json.dumps(payload).encode(), headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def smoke(url, token=None):
    def rpc(method, params=None):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params is not None:
            payload["params"] = params
        status, body = request(url, payload, token=token)
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f"{method}: HTTP {status}")
        if "error" in body:
            raise RuntimeError(f"{method}: {body['error']}")
        return body["result"]

    initialized = rpc("initialize", {
        "protocolVersion": "2025-03-26", "capabilities": {},
        "clientInfo": {"name": "sky-backcraft-desktop-e2e", "version": "1"},
    })
    tools = rpc("tools/list")["tools"]
    if not any(tool["name"] == "lab_status" for tool in tools):
        raise RuntimeError("lab_status is missing from the tool catalog")
    called = rpc("tools/call", {"name": "lab_status", "arguments": {}})
    if called.get("isError"):
        raise RuntimeError("lab_status returned a tool error")
    status = json.loads(next(c["text"] for c in called["content"] if c["type"] == "text"))
    if status["lab"] != "upbit-spot-lab" or not status["jobs"]["runner_available"]:
        raise RuntimeError("unexpected lab identity or unavailable runner")
    invalid, _ = request(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
                         token=token, extra_headers={"Host": "untrusted.invalid"})
    if invalid != 403:
        raise RuntimeError(f"unexpected Host was not rejected: HTTP {invalid}")
    return {"url": url, "protocol": initialized["protocolVersion"],
            "tool_count": len(tools), "lab_status": "PASS", "host_rejection": "PASS",
            "source_digest": status["source_digest"], "transport": status["transport"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8130/mcp")
    arguments = parser.parse_args()
    print(json.dumps(smoke(arguments.url, os.environ.get("SPOT_LAB_TEST_BEARER")), indent=2))
