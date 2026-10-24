#!/usr/bin/env python3
"""Synthetic loopback UI fixture; no exchange or strategy execution takes place.

POST /scenario accepts the existing normal/offline/slow cases and
emergency-stop-drop-once, which saves the fixture latch before dropping one ACK.
"""
import argparse
import copy
import datetime
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--review-fixture", type=Path, help="Explicit synthetic review-wire.json directory")
    parser.add_argument("--live-lifecycle", action="store_true", help="Synthetic LIVE-labelled lifecycle UI only; never contacts an exchange")
    args = parser.parse_args()
    if args.live_lifecycle and args.review_fixture:
        parser.error("Keep synthetic LIVE lifecycle and MOCK review fixtures separate")
    root = args.root.resolve()
    if not root.is_relative_to(Path("/tmp").resolve()) or not (root / "research/lab.sqlite").is_file():
        parser.error("Use the isolated temporary SQLite policy fixture")
    research = (root / "research").resolve()
    database = (research / "lab.sqlite").resolve()
    if not research.is_relative_to(root) or not database.is_relative_to(research) or database.stat().st_nlink != 1:
        parser.error("Fixture research storage must remain exclusively inside its temporary root")

    def prepare_native_read_sidecars():
        # Rust's newer bundled SQLite closes its owner after CLI reads. Prepare
        # system-SQLite WAL sidecars in this private fixture for AppKit's read-only reader.
        if args.live_lifecycle and sys.platform == "darwin":
            subprocess.run(["/usr/bin/sqlite3", str(database), "SELECT COUNT(*) FROM policies;"],
                           cwd=root, check=True, capture_output=True, timeout=10)

    prepare_native_read_sidecars()
    repository = Path(__file__).resolve().parents[2]
    binary = repository / "dist/Sky Backcraft.app/Contents/MacOS/spot-lab"
    template = json.loads((repository / "macos/tests/fixtures/AutomationWire.json").read_text())
    live_wire = json.loads((repository / "macos/tests/fixtures/P13LiveReadWire.json").read_text())
    fixture_scope = live_wire["fixture_scope"]
    if any(fixture_scope.get(key) is not False for key in (
        "provider_requests", "exchange_effects", "production_sqlite_writes",
    )):
        parser.error("P13 private-read fixture must remain synthetic and effect-free")
    if fixture_scope.get("control_mutations_are_simulated") is not True:
        parser.error("P13 trading control fixture must remain simulated")
    policy_list = json.loads((root / "policy-list-output.json").read_text())
    session = copy.deepcopy(template["session_view"])
    session["policy_ref"] = policy_list["items"][0]["head"]
    ledger = copy.deepcopy(template["ledger_page"])
    order = ledger["orders"][0]
    order.update(state="PARTIALLY_FILLED", executed_volume="0.00025", paid_fee_krw="12.5", trade_count=1)
    ledger["fills"][0].update(volume="0.00025", funds_krw="25000")
    session.update(unresolved_order_count=1, fill_count=1)
    review = None
    if args.review_fixture:
        review = json.loads((args.review_fixture / "review-wire.json").read_text())
        if any(review["fixture_scope"].get(key) is not False for key in (
            "provider_requests", "exchange_effects", "production_sqlite_writes",
        )) or review["snapshot"]["session"]["mode"] != "MOCK":
            parser.error("Review fixture must be explicit synthetic MOCK data")
        session.update(copy.deepcopy(review["snapshot"]["session"]))
        session.update(fill_count=len(review["snapshot"]["fills"]), unresolved_order_count=0)
        ledger = {
            "orders": [copy.deepcopy(o["order"]) for o in review["snapshot"]["orders"]],
            "fills": [copy.deepcopy(f["fill"]) for f in review["snapshot"]["fills"]],
            "next_sequence": None,
        }
        order = ledger["orders"][0]
    scenario = "normal"
    requests = {}
    observations = []
    lock = threading.RLock()
    control_state_path = root / "p13-trading-control-state.json"
    control_record = {
        "status": copy.deepcopy(live_wire["trading_control"]),
        "dropped_response_request_ids": [],
    }
    if args.live_lifecycle:
        control_record["status"].update(live_trading_enabled=True, state="STANDBY", order_admission_blocked=True)
    if control_state_path.exists():
        control_record = json.loads(control_state_path.read_text())

    def now():
        return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    lifecycle_state_path = root / "synthetic-live-lifecycle.json"
    dropped_write_ids = []
    live_session_recorded = False
    if args.live_lifecycle:
        identity = "live:upbit:" + "b" * 64
        session.update(mode="LIVE", account_identity=identity, unresolved_order_count=0, fill_count=0)
        session["risk"]["activation_loss_limit_krw"] = "50000"
        account = session["account_risk"]
        account.update(mock_assumptions=None, day_baseline=None, legacy_mock_baseline=None)
        account["balance"]["source"] = "UPBIT_LIVE_READ"
        account["risk"]["daily_loss_used_krw"] = None
        account["effective_loss"] = {"basis": "LIVE_ACTIVATION_OBSERVED", "limit_krw": "50000", "used_krw": "0", "admission_blocked": False, "block_reasons": []}
        account["activation_reference"] = {
            "account_identity": identity, "mode": "LIVE", "source": "CURRENT_LOCAL_OBSERVED",
            "reference_observed_at_utc": now(), "reference_received_at_utc": now(),
            "valuation_recorded_at_utc": now(), "snapshot_sequence": 1, "control_sequence": 1,
            "reference_max_feed_age_seconds": 90, "balances_digest": "c" * 64,
            "valuation_state": "VERIFIED", "valuation_digest": "d" * 64, "reference_equity_krw": "10000000",
        }
        ledger = {"orders": [], "fills": [], "next_sequence": None}
        if lifecycle_state_path.exists():
            saved = json.loads(lifecycle_state_path.read_text())
            session, requests, dropped_write_ids = saved["session"], saved["requests"], saved["dropped_write_ids"]
            live_session_recorded = True

    def persist_lifecycle():
        if args.live_lifecycle:
            temporary = lifecycle_state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps({"session": session, "requests": requests, "dropped_write_ids": dropped_write_ids}))
            os.replace(temporary, lifecycle_state_path)

    def snapshot():
        value = copy.deepcopy(session)
        value["created_at_utc"] = value["updated_at_utc"] = now()
        account = value["account_risk"]
        account["balance"]["observed_at_utc"] = account["balance"]["received_at_utc"] = now()
        account["capabilities"].update(
            can_pause=value["state"] == "RUNNING", can_resume=value["state"] == "PAUSED",
            can_stop=value["state"] != "STOPPED", can_cancel_order=order["state"] in ["OPEN", "PARTIALLY_FILLED"],
            can_submit_orders=value["state"] == "RUNNING", live_effects_enabled=False,
        )
        account["risk"]["live_grant_active"] = False
        if args.live_lifecycle:
            sending = scenario != "live-gate-off"
            grant = control_record["status"]["state"] == "RUNNING" and not control_record["status"]["order_admission_blocked"] and not control_record["status"]["emergency_latched"]
            account["feed"].update(freshness="FRESH", observation_close_at_utc=now(), received_at_utc=now())
            account["capabilities"].update(
                can_resume=value["state"] == "PAUSED" and sending and grant,
                can_cancel_order=False, can_submit_orders=value["state"] == "RUNNING" and sending and grant,
                live_effects_enabled=sending, explicit_approval_required=True,
            )
            account["risk"]["live_grant_active"] = sending and grant
            account["effective_loss"]["limit_krw"] = value["risk"].get("activation_loss_limit_krw")
            account["admission_block_reasons"] = ([] if sending else ["LIVE_TRADING_DISABLED"]) + ([] if grant else ["LIVE_GRANT_INACTIVE"])
        return value

    def write_observations():
        (root / "gui-fixture-observations.json").write_text(json.dumps({
            "scenario": scenario,
            "calls": observations,
            "mock_only": not args.live_lifecycle,
            "synthetic_live_lifecycle": args.live_lifecycle,
            "provider_requests": False, "exchange_effects": False, "production_sqlite_writes": False,
            "private_read_fixture_scope": fixture_scope,
            "fixture_control_state_path": str(control_state_path),
        }, indent=2))

    def persist_control_record():
        temporary = control_state_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(control_record, indent=2, sort_keys=True))
        os.replace(temporary, control_state_path)

    def control_projection(denied_code=None, receipt=None):
        value = copy.deepcopy(control_record["status"])
        if args.live_lifecycle:
            value["live_trading_enabled"] = scenario != "live-gate-off"
        value["observed_at_utc"] = now()
        value["denied_code"] = denied_code
        value["receipt"] = receipt
        return value

    def control_receipt(request_id, action, outcome, sequence, denied_code=None):
        return {
            "request_id": request_id,
            "action": action,
            "outcome": outcome,
            "sequence": sequence,
            "observed_at_utc": now(),
            "denied_code": denied_code,
        }

    def private_read(tool, arguments):
        if tool == "upbit_account_balances":
            value = copy.deepcopy(live_wire["account_balances"])
            value["observed_at_utc"] = now()
            value["accounts_count"] = len(value["balances"])
            return value
        if tool == "upbit_orders_open":
            value = copy.deepcopy(live_wire["open_orders"])
            page = arguments.get("page", 1)
            limit = arguments.get("limit", 100)
            if not isinstance(page, int) or isinstance(page, bool) or page < 1:
                raise ValueError("Invalid fixture open-order page")
            if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
                raise ValueError("Invalid fixture open-order limit")
            value["observed_at_utc"] = now()
            value["page"] = page
            value["limit"] = limit
            records = value["records"]
            if arguments.get("market"):
                records = [item for item in records if item["market"] == arguments["market"]]
            start = (page - 1) * limit
            value["records"] = records[start:start + limit]
            value["returned_count"] = len(value["records"])
            full = value["returned_count"] == limit
            value["next_page"] = page + 1 if full and page < 10_000 else None
            value["truncated_reason"] = "PAGE_LIMIT_REACHED" if full else None
            return value
        if tool == "upbit_orders_closed":
            value = copy.deepcopy(live_wire["closed_orders"])
            limit = arguments.get("limit", 100)
            if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
                raise ValueError("Invalid fixture closed-order limit")
            value["observed_at_utc"] = now()
            value["limit"] = limit
            value["start_time"] = arguments.get("start_time")
            value["end_time"] = arguments.get("end_time")
            records = value["records"]
            if arguments.get("market"):
                records = [item for item in records if item["market"] == arguments["market"]]
            value["records"] = records[:limit]
            value["returned_count"] = len(value["records"])
            value["truncated_reason"] = (
                "TIME_RANGE_OR_LIMIT_REACHED" if value["returned_count"] == limit else None
            )
            return value
        raise ValueError("Fixture private read tool unavailable")

    def review_query(arguments):
        if review is None:
            raise ValueError("Synthetic review fixture was not selected")
        if arguments.get("session_id") != review["snapshot"]["session"]["session_id"]:
            raise ValueError("Review session identity mismatch")
        action = arguments["action"]
        if action == "decisions":
            value = copy.deepcopy(review["decisions"])
            offset, limit = arguments["offset"], arguments["limit"]
            if not isinstance(offset, int) or offset < 0 or not isinstance(limit, int) or not 1 <= limit <= 100:
                raise ValueError("Invalid review page")
            value["items"] = value["items"][offset:offset + limit]
            value["returned_count"] = len(value["items"])
            value["next_offset"] = offset + limit if offset + limit < value["total_count"] else None
            return value
        if action == "decision":
            decision = next((d for d in review["snapshot"]["decisions"] if d["intent_id"] == arguments.get("intent_id")), None)
            if decision is None:
                raise ValueError("Unknown synthetic decision")
            value = copy.deepcopy(review["decision"])
            value["decision"] = copy.deepcopy(decision)
            value["context_candles"] = [copy.deepcopy(c) for c in review["snapshot"]["candles"] if c["candle"]["close_time_utc"] <= decision["decision_time_utc"]][-64:]
            value["orders"] = [copy.deepcopy(o["order"]) for o in review["snapshot"]["orders"] if o["order"]["intent_id"] == decision["intent_id"]]
            clients = {o["client_identifier"] for o in value["orders"]}
            value["events"] = [copy.deepcopy(e) for e in review["snapshot"]["events"] if e["detail"].get("intent_id") == decision["intent_id"] or e["detail"].get("client_identifier") in clients]
            return value
        if action in ("performance", "export_manifest"):
            return copy.deepcopy(review[action])
        if action == "export_read":
            manifest = review["export_manifest"]
            file = next((f for f in manifest["files"] if f["file"] == arguments.get("file")), None)
            if file is None or arguments.get("expected_snapshot_digest") != manifest["snapshot_digest"] or arguments.get("expected_sha256") != file["sha256"]:
                raise ValueError("INPUT_HASH_MISMATCH: synthetic manifest mismatch")
            data = (args.review_fixture / file["file_name"]).read_bytes()
            if hashlib.sha256(data).hexdigest() != file["sha256"]:
                raise ValueError("Synthetic export fixture file hash mismatch")
            offset = arguments["offset"]
            if not isinstance(offset, int) or not 0 <= offset <= len(data):
                raise ValueError("Invalid export offset")
            end = min(offset + 65536, len(data))
            chunk = data[offset:end]
            return {
                "session_id": manifest["session_id"], "mode": manifest["mode"],
                "snapshot_digest": manifest["snapshot_digest"], "file": file["file"],
                "file_name": file["file_name"], "media_type": file["media_type"],
                "total_bytes": len(data), "sha256": file["sha256"], "offset": offset,
                "bytes": len(chunk), "chunk_sha256": hashlib.sha256(chunk).hexdigest(),
                "encoding": "HEX", "data_hex": chunk.hex(), "next_offset": end if end < len(data) else None,
            }
        raise ValueError("Unsupported review action")

    def trading_control(arguments):
        action = arguments.get("action")
        current = control_record["status"]
        sequence = current["sequence"]
        if action == "status":
            return control_projection(), False
        if action in ("start", "resume"):
            request_id = arguments.get("request_id")
            if not isinstance(request_id, str) or not request_id:
                raise ValueError("Fixture trading control mutation requires request_id")
            if args.live_lifecycle:
                if arguments.get("expected_sequence") != sequence:
                    raise ValueError("Synthetic global control sequence conflict")
                valid = scenario != "live-gate-off" and not current["emergency_latched"] and current["state"] == {"start": "STANDBY", "resume": "PAUSED"}[action]
                updated = copy.deepcopy(current)
                if valid:
                    updated.update(state="RUNNING", order_admission_blocked=False, sequence=sequence + 1)
                control_record["status"] = updated
                persist_control_record()
                denial = None if valid else ("LIVE_TRADING_DISABLED" if scenario == "live-gate-off" else "INVALID_STATE")
                receipt = control_receipt(request_id, action, "ACKNOWLEDGED" if valid else "DENIED", updated["sequence"], denial)
                drop = scenario == "global-start-drop-once" and request_id not in control_record["dropped_response_request_ids"]
                if drop:
                    control_record["dropped_response_request_ids"].append(request_id)
                    persist_control_record()
                return control_projection(denial, receipt), drop
            receipt = control_receipt(
                request_id, action, "DENIED", sequence, "LIVE_TRADING_DISABLED",
            )
            return control_projection("LIVE_TRADING_DISABLED", receipt), False
        if action != "emergency_stop":
            raise ValueError("Fixture trading control action unavailable")

        request_id = arguments.get("request_id")
        expected_sequence = arguments.get("expected_sequence")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("Fixture emergency stop requires request_id")
        if not isinstance(expected_sequence, int) or isinstance(expected_sequence, bool):
            raise ValueError("Fixture emergency stop requires expected_sequence")
        if expected_sequence != sequence:
            raise ValueError("Fixture trading control sequence conflict")

        observed_at = now()
        updated = copy.deepcopy(current)
        updated.update(
            state="EMERGENCY_STOP_ACTIVE",
            sequence=sequence + 1,
            observed_at_utc=observed_at,
            order_admission_blocked=True,
            emergency_latched=True,
            denied_code=None,
            receipt=None,
        )
        control_record["status"] = updated
        dropped_ids = control_record["dropped_response_request_ids"]
        drop_response = scenario == "emergency-stop-drop-once" and request_id not in dropped_ids
        if drop_response:
            dropped_ids.append(request_id)
        persist_control_record()
        receipt = control_receipt(
            request_id, action, "ACKNOWLEDGED", updated["sequence"],
        )
        result = copy.deepcopy(updated)
        result["observed_at_utc"] = observed_at
        result["receipt"] = receipt
        return result, drop_response

    def policy_query(arguments):
        path = root / "gui-policy-query.json"
        path.write_text(json.dumps(arguments))
        result = subprocess.run(
            [str(binary), "policy-query", "--request", str(path), "--data-root", str(root / "research")],
            cwd=root, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise ValueError("Isolated policy query rejected")
        prepare_native_read_sidecars()
        return json.loads(result.stdout)

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            nonlocal scenario, live_session_recorded
            length = int(self.headers.get("Content-Length", "0"))
            if length > 262144:
                self.send_error(413)
                return
            envelope = json.loads(self.rfile.read(length))
            if self.path == "/scenario":
                with lock:
                    scenario = envelope["scenario"]
                return self.reply({"scenario": scenario})
            if self.path != "/desktop/mcp" or self.headers.get("Authorization") != "Bearer " + "a" * 64:
                self.send_error(401)
                return
            message_id = envelope.get("id")
            if envelope.get("method") == "initialize":
                return self.reply({"jsonrpc": "2.0", "id": message_id, "result": {"protocolVersion": "2025-03-26"}})
            params = envelope.get("params", {})
            tool, arguments = params.get("name"), params.get("arguments", {})
            action = arguments.get("action")
            with lock:
                observation = {"tool": tool, "action": action}
                observations.append(observation)
                try:
                    if tool == "policy_query":
                        value = policy_query(arguments)
                    elif tool == "live_order_control" and args.live_lifecycle and action == "status":
                        value = {
                            "boot_epoch": "boot-synthetic-live-gui", "live_order_control_available": True,
                            "live_trading_enabled": scenario != "live-gate-off", "sequence": 0,
                            "observed_at_utc": now(), "exchange_read_mode": "LIVE_READ",
                            "order_admission_blocked": control_record["status"]["order_admission_blocked"],
                            "emergency_latched": control_record["status"]["emergency_latched"],
                            "confirmation_required": True, "receipt": None,
                        }
                    elif tool == "trading_control":
                        value, drop_response = trading_control(arguments)
                        if drop_response:
                            observation["fixture_response"] = "APPLIED_RESPONSE_DROPPED"
                        elif action == "emergency_stop":
                            observation["fixture_response"] = "MOCK_EMERGENCY_STOP_ACK"
                        elif action in ("start", "resume"):
                            observation["fixture_response"] = "SYNTHETIC_GLOBAL_" + value["receipt"]["outcome"] if args.live_lifecycle else "LIVE_TRADING_DISABLED_DENIED"
                        else:
                            observation["fixture_response"] = "SYNTHETIC_CONTROL_PROJECTION"
                        observation["request_id"] = arguments.get("request_id")
                        observation["sequence"] = value["sequence"]
                        if drop_response:
                            write_observations()
                            self.close_connection = True
                            return
                    elif tool in (
                        "upbit_account_balances", "upbit_orders_open", "upbit_orders_closed",
                    ):
                        value = private_read(tool, arguments)
                        observation["fixture_response"] = "SYNTHETIC_PRIVATE_READ"
                    elif tool == "automation_session_query":
                        if action in ("decisions", "decision", "performance", "export_manifest", "export_read"):
                            value = review_query(arguments)
                        elif action == "list":
                            items = [] if args.live_lifecycle and not live_session_recorded else [snapshot()]
                            value = {"items": items, "returned_count": len(items), "total_count": len(items), "next_offset": None}
                        elif action == "get":
                            value = snapshot()
                        elif action == "request":
                            value = requests.get(arguments["request_id"], {"request_id": arguments["request_id"], "state": "NOT_RECORDED", "response": None, "reason": None})
                        elif action == "baseline":
                            snap = snapshot()
                            base = copy.deepcopy(snap["account_risk"]["day_baseline"])
                            day_utc = arguments.get("day_utc") or (base["day_utc"] if base else now()[:10])
                            if base and base["day_utc"] != day_utc:
                                base = None
                            reasons = list(snap["account_risk"].get("admission_block_reasons", []))
                            if base is None and "DAILY_START_BASELINE_MISSING" not in reasons:
                                reasons.append("DAILY_START_BASELINE_MISSING")
                            value = {
                                "session_id": snap["session_id"],
                                "account_identity": snap["account_identity"],
                                "mode": snap["mode"],
                                "day_utc": day_utc,
                                "admission_blocked": bool(reasons),
                                "block_reasons": reasons,
                                "baseline": base,
                            }
                        elif action == "events":
                            value = copy.deepcopy(template["event_page"])
                            for item in value["items"]:
                                item["at_utc"] = now()
                                if args.live_lifecycle:
                                    item["session_id"] = session["session_id"]
                                    item["detail"] = {"mode": "LIVE", "scope": "synthetic fixture; no orders"}
                        elif action == "ledger":
                            value = copy.deepcopy(ledger)
                            for item in value["orders"]:
                                item["created_at_utc"] = item["updated_at_utc"] = now()
                            for item in value["fills"]:
                                item["observed_at_utc"] = now()
                        else:
                            raise ValueError("Unsupported query")
                    elif tool == "automation_session_write":
                        expected_mode = "LIVE" if args.live_lifecycle else "MOCK"
                        if arguments.get("mode", expected_mode) != expected_mode:
                            raise ValueError("Fixture requires " + expected_mode)
                        if action not in ["start", "pause", "resume", "stop", "cancel_order", "record_baseline"]:
                            raise ValueError("Unsupported write")
                        if args.live_lifecycle and arguments.get("request_id") in requests:
                            raise ValueError("Duplicate synthetic mutation: query the saved request receipt")
                        if action != "start" and arguments.get("expected_sequence") != session["sequence"]:
                            raise ValueError("Fixture sequence conflict")
                        if args.live_lifecycle and action in ("cancel_order", "record_baseline"):
                            raise ValueError("LIVE fixture does not expose cancellation or baseline mutation")
                        if args.live_lifecycle and action == "resume" and not snapshot()["account_risk"]["capabilities"]["can_resume"]:
                            raise ValueError("Synthetic LIVE send gate is OFF")
                        if args.live_lifecycle and action != "start" and arguments.get("session_id") != session["session_id"]:
                            raise ValueError("Synthetic LIVE session identity mismatch")
                        if args.live_lifecycle and action == "start" and not arguments["risk"].get("activation_loss_limit_krw"):
                            raise ValueError("Choose an explicit activation loss limit for this synthetic fixture")
                        recorded_baseline = None
                        if action == "start":
                            session.update(policy_ref=arguments["policy_ref"], risk=arguments["risk"], market=arguments["market"], decision_interval=arguments["decision_interval"], state="RUNNING")
                            if args.live_lifecycle:
                                session["session_id"] = "automation-" + hashlib.sha256(arguments["request_id"].encode()).hexdigest()
                                live_session_recorded = True
                                if not snapshot()["account_risk"]["risk"]["live_grant_active"]:
                                    session["state"] = "BLOCKED_APPROVAL"
                        elif action in ["pause", "resume", "stop"]:
                            session["state"] = {"pause": "PAUSED", "resume": "RUNNING", "stop": "STOPPED"}[action]
                        elif action == "cancel_order":
                            if arguments["client_identifier"] != order["client_identifier"]:
                                raise ValueError("Fixture order is not owned")
                            order["state"] = "CANCELLED"
                        elif action == "record_baseline":
                            b_input = arguments["baseline"]
                            verified = b_input["source"] != "OPERATOR_ATTESTED"
                            block_reasons = [] if verified else ["OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION"]
                            recorded_baseline = copy.deepcopy(template["session_view"]["account_risk"]["day_baseline"])
                            recorded_baseline.update(
                                account_identity=b_input["account_identity"],
                                day_utc=b_input["day_utc"],
                                reference_time_utc=b_input["reference_time_utc"],
                                recorded_at_utc=now(),
                                source=b_input["source"],
                                verification_state="VERIFIED" if verified else "BLOCKED_INSUFFICIENT_EVIDENCE",
                                sufficient_for_risk_admission=verified,
                                total_equity_krw="10000000" if verified else None,
                                block_reasons=block_reasons,
                                request_id=arguments["request_id"],
                                operator_note=b_input.get("operator_note"),
                            )
                            session["account_risk"]["day_baseline"] = recorded_baseline
                            session["account_risk"]["admission_block_reasons"] = [] if verified else ["DAILY_START_BASELINE_BLOCKED", "BASELINE:OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION"]
                        session["sequence"] += 1
                        value = {
                            "session": snapshot(),
                            "receipt": {
                                "request_id": arguments["request_id"],
                                "action": action.upper(),
                                "outcome": "DENIED" if args.live_lifecycle and action == "start" and session["state"] == "BLOCKED_APPROVAL" else "APPLIED",
                                "sequence": session["sequence"],
                                "observed_at_utc": now(),
                                "reason": "LIVE_GRANT_INACTIVE" if args.live_lifecycle and session["state"] == "BLOCKED_APPROVAL" else None,
                            },
                            "order": copy.deepcopy(order) if action == "cancel_order" else None,
                            "baseline": copy.deepcopy(recorded_baseline),
                        }
                        requests[arguments["request_id"]] = {"request_id": arguments["request_id"], "state": "APPLIED", "response": copy.deepcopy(value), "reason": None}
                        if args.live_lifecycle:
                            observation.update(request_id=arguments["request_id"], mode=session["mode"], session_id=session["session_id"], sequence=session["sequence"], arguments=arguments)
                            drop = scenario == "live-write-drop-once" and arguments["request_id"] not in dropped_write_ids
                            if drop:
                                dropped_write_ids.append(arguments["request_id"])
                            persist_lifecycle()
                            if drop:
                                observation["fixture_response"] = "SYNTHETIC_LIVE_APPLIED_RESPONSE_DROPPED"
                                write_observations()
                                self.close_connection = True
                                return
                    else:
                        raise ValueError("Fixture tool unavailable")
                    if tool == "trading_control" and action == "status" and value["emergency_latched"]:
                        observation["fixture_response"] = "DURABLE_FIXTURE_READBACK"
                    write_observations()
                    result = {"isError": False, "content": [{"type": "text", "text": json.dumps(value)}]}
                except (ValueError, KeyError) as error:
                    write_observations()
                    result = {"isError": True, "content": [{"type": "text", "text": str(error)}]}
            if scenario == "offline" and tool == "automation_session_query":
                self.send_error(503)
                return
            if scenario == "slow" and tool == "automation_session_query":
                time.sleep(4)
            self.reply({"jsonrpc": "2.0", "id": message_id, "result": result})

        def reply(self, value):
            body = json.dumps(value).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except BrokenPipeError:
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print(json.dumps({"pid": os.getpid(), "port": server.server_port, "root": str(root), "scope": "UI presentation fixture only, not backend execution proof"}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
