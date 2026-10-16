#!/usr/bin/env python3
"""Current-binary private review HTTP journey over synthetic stored MOCK facts."""
import argparse
import csv
import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request


def sql_time(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat(timespec="seconds")


def seed(database, snapshot, opening_balances):
    """Offline fixture initialization only; no runtime shares this connection."""
    session = snapshot["session"]
    assert session["mode"] == "MOCK" and session["state"] == "STOPPED"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA foreign_keys=ON")

    def insert(table, row):
        columns = ",".join(row)
        slots = ",".join("?" for _ in row)
        connection.execute(f"INSERT INTO {table}({columns}) VALUES ({slots})", list(row.values()))

    sid = session["session_id"]
    insert("automation_sessions", {
        "session_id": sid, "start_request_id": "synthetic-review-start", "account_identity": "mock:upbit-default",
        "mode": "MOCK", "state": "STOPPED", "sequence": 0,
        "policy_ref_json": json.dumps(session["policy_ref"]), "frozen_policy_json": json.dumps(snapshot["frozen_policy"]),
        "market": session["market"], "decision_interval": session["decision_interval"],
        "risk_json": json.dumps(session["risk"]), "checkpoint_json": "{}",
        "last_finalized_candle_at_utc": sql_time(snapshot["decisions"][-1]["decision_time_utc"]),
        "unresolved_order_count": 0, "fill_count": len(snapshot["fills"]),
        "created_at_utc": sql_time(session["created_at_utc"]), "updated_at_utc": sql_time(session["updated_at_utc"]),
    })
    for candle in snapshot["candles"]:
        insert("automation_candles", {
            "session_id": sid, "observation_id": candle["id"], "close_time_utc": sql_time(candle["candle"]["close_time_utc"]),
            "content_digest": candle["content_digest"], "candle_json": json.dumps(candle), "checkpoint_json": "{}",
        })
    for d in snapshot["decisions"]:
        insert("automation_intents", {
            "intent_id": d["intent_id"], "session_id": sid, "observation_id": d["observation_id"],
            "decision_time_utc": sql_time(d["decision_time_utc"]), "action": d["action"], "state": d["state"],
            "evaluation_json": json.dumps(d["evaluation"]), "created_at_utc": sql_time(d["recorded_at_utc"]),
        })
    for row in snapshot["orders"]:
        o = row["order"]
        insert("automation_orders", {
            "client_identifier": o["client_identifier"], "intent_id": o["intent_id"], "session_id": sid,
            "account_identity": "mock:upbit-default", "order_request_json": "{}", "rules_snapshot_json": "{}",
            "request_digest": "a" * 64, "market": o["market"], "side": o["side"], "state": o["state"],
            "venue_order_uuid": o["venue_order_uuid"], "requested_notional_krw": o["requested_notional_krw"],
            "decision_mark_krw": row["decision_mark_krw"], "reserve_quote_krw": "0", "reserve_asset": "BTC",
            "reserved_asset_volume": "0", "reserved_fee_krw": o["reserved_fee_krw"], "remaining_fee_krw": "0",
            "executed_volume": o["executed_volume"], "remaining_volume": "0", "paid_fee_krw": o["paid_fee_krw"],
            "locked_amount": "0", "locked_currency": "KRW" if o["side"] == "BID" else session["market"].split("-")[1], "trades_count": o["trade_count"],
            "venue_created_at_utc": sql_time(row["venue_created_at_utc"]),
            "created_at_utc": sql_time(o["created_at_utc"]), "updated_at_utc": sql_time(o["updated_at_utc"]),
        })
    for row in snapshot["fills"]:
        f = row["fill"]
        insert("automation_fills", {
            "trade_uuid": f["trade_uuid"], "client_identifier": f["client_identifier"], "session_id": sid,
            "market": f["market"], "side": f["side"], "price_krw": f["price_krw"], "volume": f["volume"],
            "funds_krw": f["funds_krw"], "trade_time_utc": sql_time(row["stored_trade_time_utc"]),
            "source_digest": row["source_digest"], "created_at_utc": sql_time(row["recorded_at_utc"]),
        })
    for f in snapshot["fee_accruals"]:
        insert("automation_fee_accruals", {**f, "observed_at_utc": sql_time(f["observed_at_utc"])})
    for event in snapshot["events"]:
        insert("automation_session_events", {"session_id": sid, "sequence": event["sequence"], "kind": event["kind"],
            "at_utc": sql_time(event["at_utc"]), "detail_json": json.dumps(event["detail"])})
    opening = snapshot.get("opening_inventory")
    if opening is not None:
        insert("automation_account_snapshots", {
            "snapshot_sequence": opening["snapshot_sequence"], "account_identity": "mock:upbit-default",
            "observed_at_utc": sql_time(opening["observed_at_utc"]), "source_mode": "MOCK",
            "balances_json": json.dumps(opening_balances), "balances_digest": opening["balances_digest"],
            "created_at_utc": sql_time(opening["observed_at_utc"]),
        })
    connection.commit()
    connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    wire = json.loads((args.fixture / "review-wire.json").read_text())
    assert all(wire["fixture_scope"][k] is False for k in ["provider_requests", "exchange_effects", "production_sqlite_writes"])
    snapshot = wire["snapshot"]
    checks = []
    environment = {k: os.environ[k] for k in ["PATH", "HOME", "TMPDIR", "LANG"] if k in os.environ}
    environment["SERVICE_DESKTOP_ACCESS_TOKEN"] = "a" * 64
    process = None
    log = None
    counter = 0
    with tempfile.TemporaryDirectory(prefix="automation-review-http-", dir="/tmp") as directory:
        root = Path(directory)
        request = root / "policy.json"
        request.write_text(json.dumps({"action": "create", "request_id": "review-init", "definition": snapshot["frozen_policy"]["definition"]}))
        subprocess.run([str(binary), "policy-write", "--request", str(request), "--data-root", str(root / "research")],
                       cwd=root, env=environment, capture_output=True, text=True, check=True, timeout=20)
        seed(root / "research/lab.sqlite", snapshot, wire["opening_balances"])
        sock = socket.socket(); sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]; sock.close()

        def rpc(method, params):
            nonlocal counter
            counter += 1
            req = urllib.request.Request(f"http://127.0.0.1:{port}/desktop/mcp", data=json.dumps({"jsonrpc": "2.0", "id": counter, "method": method, "params": params}).encode(), headers={"Authorization": "Bearer " + "a" * 64, "Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"})
            with urllib.request.urlopen(req, timeout=20) as response:
                data = json.load(response)
            if "error" in data:
                raise AssertionError(data["error"])
            return data["result"]

        def call(action, **extra):
            result = rpc("tools/call", {"name": "automation_session_query", "arguments": {"action": action, "session_id": snapshot["session"]["session_id"], **extra}})
            assert not result.get("isError"), result
            return json.loads(result["content"][0]["text"])

        def start(label):
            nonlocal process, log
            log = (evidence / f"{label}.log").open("w")
            process = subprocess.Popen([str(binary), "mcp-serve", "--bind", "127.0.0.1", "--port", str(port), "--data-root", str(root / "research")], cwd=root, env=environment, stdout=log, stderr=log)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise AssertionError("isolated server exited")
                try:
                    rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "mock-review-acceptance", "version": "1"}})
                    return
                except OSError:
                    time.sleep(0.1)
            raise AssertionError("isolated readiness timeout")

        def stop():
            nonlocal process, log
            if process and process.poll() is None:
                process.send_signal(signal.SIGINT)
                try: process.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=5)
                    raise AssertionError("fixture did not drain")
            if log: log.close()
            process = None; log = None

        try:
            start("first")
            status = rpc("tools/call", {"name": "trading_control", "arguments": {"action": "status"}})
            assert json.loads(status["content"][0]["text"])["live_trading_enabled"] is False
            page = call("decisions", offset=0, limit=2)
            assert page["returned_count"] == 2 and page["total_count"] == len(snapshot["decisions"]) and page["next_offset"] == 2
            first = snapshot["decisions"][0]
            detail = call("decision", intent_id=first["intent_id"])
            assert detail["decision"]["evaluation"] == first["evaluation"]
            assert detail["decision"]["candle"] == first["candle"]
            assert all(c["candle"]["close_time_utc"] <= first["decision_time_utc"] for c in detail["context_candles"])
            performance = call("performance")
            assert Decimal(performance["realized_pnl_krw"]["value"]) == Decimal("47.5")
            assert Decimal(performance["unrealized_pnl_krw"]["value"]) == Decimal("39")
            assert Decimal(performance["total_pnl_krw"]["value"]) == Decimal("86.5")
            assert Decimal(performance["net_quote_cashflow_krw"]) == Decimal("-53.5")
            assert performance["return_ratio"]["value"] is None
            manifest = call("export_manifest")
            received = evidence / "received"; received.mkdir(exist_ok=False)
            for file in manifest["files"]:
                data = bytearray(); offset = 0
                while True:
                    chunk = call("export_read", file=file["file"], offset=offset, expected_sha256=file["sha256"], expected_snapshot_digest=manifest["snapshot_digest"])
                    decoded = bytes.fromhex(chunk["data_hex"])
                    assert chunk["offset"] == offset and len(decoded) == chunk["bytes"]
                    assert hashlib.sha256(decoded).hexdigest() == chunk["chunk_sha256"]
                    data.extend(decoded)
                    if chunk["next_offset"] is None: break
                    assert chunk["next_offset"] > offset
                    offset = chunk["next_offset"]
                assert len(data) == file["bytes"] and hashlib.sha256(data).hexdigest() == file["sha256"]
                (received / file["file_name"]).write_bytes(data)
            with (received / "decisions.csv").open(newline="") as stream:
                decisions = list(csv.DictReader(stream))
            assert len(decisions) == len(snapshot["decisions"]) and all(d["mode"] == "MOCK" for d in decisions)
            assert json.loads(decisions[0]["evaluation_json"]) == first["evaluation"]
            checks.extend(["actual private HTTP decision pagination", "original candle/evaluation without re-evaluation", "no future candle context", "hand-computed exact fee-inclusive P&L", "cashflow distinct from profit", "unknown capital not zero return", "six complete hash-verified export files", "CSV standard-library roundtrip"])
            bad = rpc("tools/call", {"name": "automation_session_query", "arguments": {"action": "export_read", "session_id": snapshot["session"]["session_id"], "file": manifest["files"][0]["file"], "offset": 0, "expected_sha256": "0" * 64, "expected_snapshot_digest": manifest["snapshot_digest"]}})
            assert bad.get("isError") and "INPUT_HASH_MISMATCH" in bad["content"][0]["text"]
            checks.append("wrong file hash rejected")
            stop(); start("restarted")
            assert call("decision", intent_id=first["intent_id"]) == detail
            assert call("export_manifest") == manifest
            checks.append("restart preserves original review and export snapshot")
            receipt = {"status": "PASS_SCOPED_SYNTHETIC_MOCK_HTTP", "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "checks": checks, "mode": "MOCK", "live_orders_enabled": False, "fixture_scope": wire["fixture_scope"], "basis": "Synthetic persisted stored facts, not provider or strategy-execution proof", "received_directory": str(received)}
            (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
            print(json.dumps(receipt, indent=2))
        finally:
            stop()


if __name__ == "__main__":
    main()
