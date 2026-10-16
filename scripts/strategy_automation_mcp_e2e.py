#!/usr/bin/env python3
"""Private loopback MCP lifecycle/restart check, isolated DB and MOCK orders only."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(131072), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--verify-review", action="store_true", help="Verify stored review of the actual MOCK evaluator and fill")
    args = parser.parse_args()
    binary = args.binary.resolve()
    binary_digest = file_sha256(binary)
    script_digest = file_sha256(Path(__file__).resolve())
    root = Path(tempfile.mkdtemp(prefix="strategy-automation-mcp-", dir="/tmp"))
    args.evidence.mkdir(parents=True, exist_ok=True)
    checks = []
    environment = {key: os.environ[key] for key in ["PATH", "HOME", "TMPDIR", "LANG"] if key in os.environ}
    environment["SERVICE_DESKTOP_ACCESS_TOKEN"] = "a" * 64
    definition = {
        "schema_version": "1.0", "name": "Mock automation lifecycle fixture",
        "description": "Small deterministic target; synthetic service tests prove fill math separately.",
        "program": {"kind": "RULES", "program": {
            "indicators": [{"id": "close", "indicator": {"kind": "CLOSE"}}], "states": [],
            "rules": [{"id": "small_long", "condition": {"op": "COMPARE", "comparison": "GT", "left": {"op": "INDICATOR", "id": "close"}, "right": {"op": "CONSTANT", "value": 0.0}},
                "target": {"kind": "WEIGHT", "value": {"op": "CONSTANT", "value": 0.001}}}],
            "fallback": {"kind": "HOLD"}, "signal_expiry": {"kind": "END_OF_RANGE"},
        }},
    }
    request_file = root / "policy.json"
    request_file.write_text(json.dumps({"action": "create", "request_id": "automation-mcp-policy", "definition": definition}))
    policy_result = subprocess.run([str(binary), "policy-write", "--request", str(request_file), "--data-root", str(root / "research")], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    if policy_result.returncode:
        raise RuntimeError("Isolated policy creation failed: " + policy_result.stderr)
    policy = json.loads(policy_result.stdout)

    def find_reference(value):
        if isinstance(value, dict):
            if {"policy_id", "revision_id", "definition_digest"}.issubset(value):
                return {key: value[key] for key in ["policy_id", "revision_id", "definition_digest"]}
            for item in value.values():
                result = find_reference(item)
                if result:
                    return result
        return None

    reference = find_reference(policy)
    assert reference, "Policy write must return a real immutable revision"
    port_socket = socket.socket()
    port_socket.bind(("127.0.0.1", 0))
    port = port_socket.getsockname()[1]
    port_socket.close()
    process = None
    log = None
    protocol = "2025-03-26"
    counter = 0

    def rpc(method, params):
        nonlocal counter
        counter += 1
        body = json.dumps({"jsonrpc": "2.0", "id": counter, "method": method, "params": params}).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{port}/desktop/mcp", data=body, headers={
            "Authorization": "Bearer " + "a" * 64, "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": protocol,
        })
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)["result"]

    def call(tool, arguments, allow_error=False):
        result = rpc("tools/call", {"name": tool, "arguments": arguments})
        if result.get("isError"):
            if allow_error:
                return result
            raise RuntimeError("Mock MCP rejected: " + str(result.get("content")))
        return json.loads(result["content"][0]["text"])

    def start_server(name, data_root=None):
        nonlocal process, log, protocol
        active_data_root = data_root if data_root is not None else (root / "research")
        log = (args.evidence / (name + ".log")).open("w")
        process = subprocess.Popen([str(binary), "mcp-serve", "--bind", "127.0.0.1", "--port", str(port), "--data-root", str(active_data_root)], cwd=root, env=environment, stdout=log, stderr=log)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Isolated MCP process exited during startup")
            try:
                result = rpc("initialize", {"protocolVersion": protocol, "capabilities": {}, "clientInfo": {"name": "automation-mock-restart-e2e", "version": "1"}})
                protocol = result["protocolVersion"]
                return
            except (OSError, KeyError):
                time.sleep(0.1)
        raise RuntimeError("Isolated MCP readiness deadline exceeded")

    def stop_server():
        nonlocal process
        if process and process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=35)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError("Owned fixture MCP did not drain")
        if log:
            log.close()
        process = None

    def verify(name, condition):
        assert condition, name
        checks.append({"name": name, "result": "PASS"})

    try:
        start_server("first-process")
        catalog = rpc("tools/list", {})
        verify("private automation tool discovery", {"automation_session_write", "automation_session_query"}.issubset({item["name"] for item in catalog["tools"]}))
        status = call("trading_control", {"action": "status"})
        verify("production live sending off before mock start", not status["live_trading_enabled"])
        risk = {"max_order_notional_krw": "100000", "max_daily_notional_krw": "1000000", "max_asset_exposure_krw": "500000", "max_daily_loss_krw": "50000", "max_open_orders": 2, "max_orders_per_interval": 1, "max_balance_age_seconds": 30, "max_feed_age_seconds": 90}
        start = {"action": "start", "request_id": "automation-mcp-start", "policy_ref": reference, "market": "KRW-BTC", "decision_interval": "m1", "mode": "MOCK", "risk": risk}
        response = call("automation_session_write", start)
        session_id = response["session"]["session_id"]
        verify("explicit mock start", response["session"]["mode"] == "MOCK")
        repeated = call("automation_session_write", start)
        verify("start identity replay", repeated["session"]["session_id"] == session_id and repeated["receipt"]["request_id"] == start["request_id"])
        readback = call("automation_session_query", {"action": "request", "request_id": start["request_id"]})
        verify("durable start readback", readback["state"] == "APPLIED" and readback["response"]["session"]["session_id"] == session_id)

        today_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        baseline_status_initial = call("automation_session_query", {"action": "baseline", "session_id": session_id, "day_utc": today_utc})
        if baseline_status_initial["baseline"] is None:
            current_session = call("automation_session_query", {"action": "get", "session_id": session_id})
            record_cmd = {
                "action": "record_baseline",
                "request_id": "automation-mcp-record-baseline",
                "session_id": session_id,
                "expected_sequence": current_session["sequence"],
                "baseline": {
                    "account_identity": "mock:upbit-default",
                    "day_utc": today_utc,
                    "reference_time_utc": today_utc + "T00:00:00Z",
                    "source": "DURABLE_MOCK_VENUE_SEED",
                    "assets": [
                        {"currency": "KRW", "free": "10000000", "locked": "0", "valuation_basis": "QUOTE_CURRENCY_PARITY"},
                        {"currency": "BTC", "free": "0", "locked": "0", "valuation_basis": "ZERO_BALANCE"},
                    ],
                    "operator_note": "explicit MCP mock day baseline",
                },
            }
            baseline_resp = call("automation_session_write", record_cmd)
            verify(
                "record_baseline verified over MCP",
                baseline_resp["receipt"]["action"] == "RECORD_BASELINE"
                and baseline_resp["baseline"]["verification_state"] == "VERIFIED"
                and baseline_resp["baseline"]["sufficient_for_risk_admission"] is True,
            )
            baseline_replay = call("automation_session_write", record_cmd)
            verify(
                "record_baseline idempotent replay",
                baseline_replay["receipt"]["outcome"] == "REPLAYED"
                and baseline_replay["baseline"]["baseline_id"] == baseline_resp["baseline"]["baseline_id"],
            )
            baseline_req_readback = call("automation_session_query", {"action": "request", "request_id": record_cmd["request_id"]})
            verify(
                "durable record_baseline request readback",
                baseline_req_readback["state"] == "APPLIED"
                and baseline_req_readback["response"]["baseline"]["baseline_digest"] == baseline_resp["baseline"]["baseline_digest"],
            )

        baseline_status_verified = call("automation_session_query", {"action": "baseline", "session_id": session_id, "day_utc": today_utc})
        verify(
            "baseline query returns verified day baseline",
            baseline_status_verified["baseline"] is not None
            and baseline_status_verified["baseline"]["verification_state"] == "VERIFIED"
            and baseline_status_verified["baseline"]["sufficient_for_risk_admission"] is True
            and "DAILY_START_BASELINE_MISSING" not in baseline_status_verified["block_reasons"]
            and "DAILY_START_BASELINE_BLOCKED" not in baseline_status_verified["block_reasons"],
        )

        current_session = call("automation_session_query", {"action": "get", "session_id": session_id})
        dup_err = call(
            "automation_session_write",
            {
                "action": "record_baseline",
                "request_id": "automation-mcp-baseline-dup",
                "session_id": session_id,
                "expected_sequence": current_session["sequence"],
                "baseline": {
                    "account_identity": "mock:upbit-default",
                    "day_utc": today_utc,
                    "reference_time_utc": today_utc + "T00:00:00Z",
                    "source": "DURABLE_MOCK_VENUE_SEED",
                    "assets": [{"currency": "KRW", "free": "10000000", "locked": "0", "valuation_basis": "QUOTE_CURRENCY_PARITY"}],
                },
            },
            allow_error=True,
        )
        verify("duplicate day baseline rejected", dup_err.get("isError") is True)

        wrong_acct_err = call(
            "automation_session_write",
            {
                "action": "record_baseline",
                "request_id": "automation-mcp-baseline-wrong-acct",
                "session_id": session_id,
                "expected_sequence": current_session["sequence"],
                "baseline": {
                    "account_identity": "live:upbit:" + ("b" * 64),
                    "day_utc": today_utc,
                    "reference_time_utc": today_utc + "T00:00:00Z",
                    "source": "OPERATOR_ATTESTED",
                    "assets": [{"currency": "KRW", "free": "10000000", "locked": "0", "valuation_basis": "QUOTE_CURRENCY_PARITY"}],
                },
            },
            allow_error=True,
        )
        verify("wrong account_identity baseline rejected", wrong_acct_err.get("isError") is True)

        wrong_time_err = call(
            "automation_session_write",
            {
                "action": "record_baseline",
                "request_id": "automation-mcp-baseline-wrong-time",
                "session_id": session_id,
                "expected_sequence": current_session["sequence"],
                "baseline": {
                    "account_identity": "mock:upbit-default",
                    "day_utc": "2026-01-01",
                    "reference_time_utc": "2026-01-01T00:00:00Z",
                    "source": "OPERATOR_ATTESTED",
                    "assets": [{"currency": "KRW", "free": "10000000", "locked": "0", "valuation_basis": "QUOTE_CURRENCY_PARITY"}],
                },
            },
            allow_error=True,
        )
        verify("mismatched date baseline rejected", wrong_time_err.get("isError") is True)

        # Exercise the native startup status read before waiting for a finalized
        # public candle. Its LIVE configuration projection must not pause MOCK.
        call("trading_control", {"action": "status"})
        deadline = time.monotonic() + 90
        while True:
            mock_ledger = call("automation_session_query", {"action": "ledger", "session_id": session_id, "after_sequence": None, "limit": 100})
            if mock_ledger["fills"]:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("MOCK fill deadline after LIVE-disabled status read: " + json.dumps(mock_ledger))
            time.sleep(1)
        verify("live-disabled status preserves mock fill path", len(mock_ledger["fills"]) == 1 and mock_ledger["orders"][0]["state"] == "FILLED")
        for action in ["pause", "resume", "stop"]:
            current = call("automation_session_query", {"action": "get", "session_id": session_id})
            command = {"action": action, "request_id": "automation-mcp-" + action, "session_id": session_id, "expected_sequence": current["sequence"]}
            response = call("automation_session_write", command)
            verify(action + " exact identity", response["session"]["session_id"] == session_id and response["receipt"]["request_id"] == command["request_id"])
        verify("stopped state", response["session"]["state"] == "STOPPED")
        ledger_before = call("automation_session_query", {"action": "ledger", "session_id": session_id, "after_sequence": None, "limit": 100})
        review_before = None
        if args.verify_review:
            page = call("automation_session_query", {"action": "decisions", "session_id": session_id, "offset": 0, "limit": 100})
            owned_intent = ledger_before["orders"][0]["intent_id"]
            verify("actual MOCK fill intent present in review", any(d["intent_id"] == owned_intent for d in page["items"]))
            detail = call("automation_session_query", {"action": "decision", "session_id": session_id, "intent_id": owned_intent})
            evaluation = detail["decision"]["evaluation"]
            evaluated = evaluation.get("policy_evaluation", evaluation)
            verify("original actual evaluator indicator and reasons retained", bool(evaluated.get("indicators")) and bool(evaluated.get("reasons")))
            verify("review owns actual MOCK order", detail["orders"] == ledger_before["orders"])
            report = call("automation_session_query", {"action": "performance", "session_id": session_id})
            verify("actual MOCK performance provenance and fill coverage", report["session"]["mode"] == "MOCK" and report["fill_count"] == len(ledger_before["fills"]))
            manifest = call("automation_session_query", {"action": "export_manifest", "session_id": session_id})
            verify("actual MOCK report and export same snapshot", manifest["snapshot_digest"] == report["snapshot_digest"])
            review_before = {"detail": detail, "performance": report, "manifest": manifest}
            (args.evidence / "actual-evaluator-review.json").write_text(json.dumps(review_before, indent=2) + "\n")
        baseline_before = call("automation_session_query", {"action": "baseline", "session_id": session_id, "day_utc": today_utc})
        status = call("trading_control", {"action": "status"})
        verify("production live sending off", not status["live_trading_enabled"])
        stop_server()
        start_server("restarted-process")
        restored = call("automation_session_query", {"action": "get", "session_id": session_id})
        verify("restart preserves stopped", restored["state"] == "STOPPED" and restored["mode"] == "MOCK")
        ledger_after = call("automation_session_query", {"action": "ledger", "session_id": session_id, "after_sequence": None, "limit": 100})
        verify("restart preserves order/fill identity", ledger_before["orders"] == ledger_after["orders"] and ledger_before["fills"] == ledger_after["fills"])
        if args.verify_review:
            verify("restart preserves actual evaluator review", call("automation_session_query", {"action": "decision", "session_id": session_id, "intent_id": owned_intent}) == review_before["detail"])
            verify("restart preserves actual review export snapshot", call("automation_session_query", {"action": "export_manifest", "session_id": session_id}) == review_before["manifest"])
        baseline_after = call("automation_session_query", {"action": "baseline", "session_id": session_id, "day_utc": today_utc})
        verify("restart preserves verified day baseline", baseline_before["baseline"] == baseline_after["baseline"])
        events = call("automation_session_query", {"action": "events", "session_id": session_id, "after_sequence": None, "limit": 100})
        verify("durable lifecycle events", len(events["items"]) >= 4)
        stop_server()

        # Verify BLOCKED_INSUFFICIENT_EVIDENCE baseline persistence and restart recovery on an isolated second DB root.
        blocked_root = Path(tempfile.mkdtemp(prefix="strategy-automation-mcp-blocked-baseline-", dir="/tmp"))
        blocked_policy_file = blocked_root / "policy.json"
        blocked_policy_file.write_text(json.dumps({"action": "create", "request_id": "automation-mcp-blocked-policy", "definition": definition}))
        blocked_policy_res = subprocess.run([str(binary), "policy-write", "--request", str(blocked_policy_file), "--data-root", str(blocked_root / "research")], cwd=blocked_root, env=environment, capture_output=True, text=True, timeout=20)
        if blocked_policy_res.returncode:
            raise RuntimeError("Isolated blocked-root policy creation failed: " + blocked_policy_res.stderr)
        blocked_ref = find_reference(json.loads(blocked_policy_res.stdout))
        start_server("blocked-baseline-first-process", data_root=blocked_root / "research")
        blocked_start = call(
            "automation_session_write",
            {"action": "start", "request_id": "blocked-mcp-start", "policy_ref": blocked_ref, "market": "KRW-BTC", "decision_interval": "m1", "mode": "MOCK", "risk": risk},
        )
        blocked_session_id = blocked_start["session"]["session_id"]
        blocked_paused = call(
            "automation_session_write",
            {"action": "pause", "request_id": "blocked-mcp-pause", "session_id": blocked_session_id, "expected_sequence": blocked_start["session"]["sequence"]},
        )
        missing_status = call("automation_session_query", {"action": "baseline", "session_id": blocked_session_id, "day_utc": today_utc})
        verify(
            "missing day baseline blocks admission",
            missing_status["baseline"] is None
            and missing_status["admission_blocked"] is True
            and "DAILY_START_BASELINE_MISSING" in missing_status["block_reasons"],
        )
        blocked_record = call(
            "automation_session_write",
            {
                "action": "record_baseline",
                "request_id": "blocked-mcp-record-operator-attested",
                "session_id": blocked_session_id,
                "expected_sequence": blocked_paused["session"]["sequence"],
                "baseline": {
                    "account_identity": "mock:upbit-default",
                    "day_utc": today_utc,
                    "reference_time_utc": today_utc + "T00:00:00Z",
                    "source": "OPERATOR_ATTESTED",
                    "assets": [
                        {"currency": "KRW", "free": "10000000", "locked": "0", "valuation_basis": "QUOTE_CURRENCY_PARITY"},
                        {"currency": "BTC", "free": "0", "locked": "0", "valuation_basis": "ZERO_BALANCE"},
                    ],
                    "operator_note": "operator attested without exchange midnight proof",
                },
            },
        )
        blocked_status = call("automation_session_query", {"action": "baseline", "session_id": blocked_session_id, "day_utc": today_utc})
        verify(
            "operator_attested baseline recorded as BLOCKED_INSUFFICIENT_EVIDENCE",
            blocked_record["baseline"]["verification_state"] == "BLOCKED_INSUFFICIENT_EVIDENCE"
            and blocked_record["baseline"]["sufficient_for_risk_admission"] is False
            and "OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION" in blocked_record["baseline"]["block_reasons"]
            and blocked_status["baseline"] == blocked_record["baseline"]
            and blocked_status["admission_blocked"] is True
            and "DAILY_START_BASELINE_BLOCKED" in blocked_status["block_reasons"],
        )
        stop_server()
        start_server("blocked-baseline-restarted-process", data_root=blocked_root / "research")
        blocked_after = call("automation_session_query", {"action": "baseline", "session_id": blocked_session_id, "day_utc": today_utc})
        verify(
            "restart preserves BLOCKED_INSUFFICIENT_EVIDENCE day baseline and block reasons",
            blocked_after["baseline"] == blocked_record["baseline"]
            and blocked_after["admission_blocked"] is True
            and "DAILY_START_BASELINE_BLOCKED" in blocked_after["block_reasons"],
        )
        stop_server()

        if file_sha256(binary) != binary_digest:
            raise RuntimeError("Helper bytes changed during verification")
        if file_sha256(Path(__file__).resolve()) != script_digest:
            raise RuntimeError("Verification script changed during execution")
        receipt = {"time_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "root": str(root), "blocked_root": str(blocked_root), "port": port, "session_id": session_id, "blocked_session_id": blocked_session_id, "checks": checks,
            "binary_path": str(binary), "binary_sha256": binary_digest, "verification_script_sha256": script_digest,
            "scope": "Actual private loopback MCP + public finalized candle + one durable MOCK fill + verified/blocked day baseline input, query, rejection, and process restart.",
            "actual_order_or_cancel_calls": False, "credentials_inherited": False, "original_server_restarted": False}
        (args.evidence / "mcp-process-restart-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps({"checks": len(checks), "result": "PASS", "root": str(root), "blocked_root": str(blocked_root)}))
    finally:
        stop_server()


if __name__ == "__main__":
    main()
