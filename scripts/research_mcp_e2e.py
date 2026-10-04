#!/usr/bin/env python3
"""Exercise all durable research features through real loopback MCP and SQLite."""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


TERMINAL_JOBS = {"COMPLETED", "PARTIAL", "FAILED", "CANCELLED", "INTERRUPTED", "BLOCKED"}


def current_source_digest():
    workspace = Path(__file__).resolve().parent.parent
    crate = workspace / "crates" / "lab"
    sources = sorted(
        [path for path in (crate / "src").rglob("*") if path.suffix in {".rs", ".sql"}]
        + [crate / "build.rs", crate / "Cargo.toml", workspace / "Cargo.toml"]
    )
    digest = hashlib.sha256()
    for path in sources:
        digest.update(path.relative_to(workspace).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


class McpClient:
    def __init__(self, url):
        self.url = url
        self.next_id = 1

    def rpc(self, method, params=None):
        request_id = self.next_id
        self.next_id += 1
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        request = urllib.request.Request(
            self.url,
            json.dumps(payload).encode(),
            {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "User-Agent": "SkyBackcraft-Research-E2E/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.load(response)
        if body.get("id") != request_id:
            raise RuntimeError(f"{method}: mismatched JSON-RPC response ID")
        if "error" in body:
            raise RuntimeError(f"{method}: {body['error']}")
        return body["result"]

    def initialize(self):
        return self.rpc(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "sky-backcraft-research-e2e", "version": "1"},
            },
        )

    def call(self, name, arguments):
        result = self.rpc("tools/call", {"name": name, "arguments": arguments})
        text = next(
            (item["text"] for item in result.get("content", []) if item.get("type") == "text"),
            None,
        )
        if result.get("isError"):
            raise RuntimeError(f"{name}: {text or 'tool returned an error'}")
        if text is None:
            raise RuntimeError(f"{name}: missing text result")
        return json.loads(text)


class IsolatedServer:
    def __init__(self, binary, root, log_path, public_no_auth=True):
        self.binary = binary
        self.root = root
        self.log_path = log_path
        self.public_no_auth = public_no_auth
        self.process = None
        self.port = None
        self.log = None

    def start(self):
        if self.process is not None:
            raise RuntimeError("server already started")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.log = self.log_path.open("a+", encoding="utf-8")
        arguments = [
                str(self.binary),
                "mcp-serve",
                "--bind",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--data-root",
                str(self.root),
            ]
        if self.public_no_auth:
            arguments.append("--public-no-auth")
        self.process = subprocess.Popen(
            arguments,
            stdout=self.log,
            stderr=self.log,
        )
        client = McpClient(f"http://127.0.0.1:{self.port}/mcp")

        def ready():
            if self.process.poll() is not None:
                self.log.flush()
                tail = self.log_path.read_text(errors="replace")[-2000:]
                raise RuntimeError(
                    f"MCP server exited {self.process.returncode} during startup: {tail}"
                )
            client.initialize()
            return client

        return wait_for("MCP server readiness", ready, 20)

    def stop(self):
        if self.process is None:
            return
        process = self.process
        self.process = None
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=40)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait()
            raise RuntimeError("MCP server did not join after SIGINT") from error
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None
        if process.returncode != 0:
            raise RuntimeError(f"MCP server exited {process.returncode}")


def wait_for(label, observe, timeout, interval=0.1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = observe()
            if value is not None:
                return value
        except (ConnectionError, urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
        time.sleep(interval)
    suffix = f": {last_error}" if last_error else ""
    raise RuntimeError(f"{label} timed out after {timeout}s{suffix}")


def latest_attempt(job):
    attempts = job["job"]["attempts"]
    if not attempts:
        raise RuntimeError("durable job has no attempts")
    return attempts[-1]


def wait_job(client, job_id, timeout=180):
    def observe():
        job = client.call("job_control", {"action": "get", "job_id": job_id})
        state = latest_attempt(job)["state"]["state"]
        if state not in TERMINAL_JOBS:
            return None
        if state not in {"COMPLETED", "PARTIAL"}:
            raise RuntimeError(f"job {job_id} ended {state}: {latest_attempt(job)['state']}")
        return job

    return wait_for(f"job {job_id}", observe, timeout)


def submit_and_wait(client, tool, arguments, timeout=180):
    submitted = client.call(tool, arguments)
    job_id = submitted["job"]["id"]
    return wait_job(client, job_id, timeout)


def output_value(job, kind, field):
    output = latest_attempt(job)["state"].get("output", {})
    if output.get("kind") != kind:
        raise RuntimeError(f"expected {kind} output, got {output}")
    return output[field]


def suite(client, action, timeout=300):
    suite_id = action.get("suite_id")
    if action["action"] == "create":
        created = client.call("research_suite", action)
        suite_id = created["id"]
    else:
        created = None

    def observe():
        summary = client.call("research_suite", {"action": "get", "suite_id": suite_id})
        if summary["status"] in {"completed", "blocked"}:
            return summary
        return None

    return created, wait_for(f"suite {suite_id}", observe, timeout, 0.2)


def revised_policy(client, policy_id, definition, request_id):
    policies = client.call(
        "policy_query", {"action": "list", "after_policy_id": None, "limit": 20}
    )
    head = next(item["head"] for item in policies["items"] if item["policy_id"] == policy_id)
    revision = client.call(
        "policy_write",
        {
            "action": "revise",
            "request_id": request_id,
            "policy_id": policy_id,
            "expected_parent_revision_id": head["revision_id"],
            "definition": definition,
        },
    )
    return revision["snapshot"]["reference"]


def experiment(dataset_id, policy_refs, start, end):
    scenario = (
        "Explicit E2E scenario only: fee and slippage are declared inputs; "
        "this run makes no profitability or historical-rule claim."
    )
    return {
        "schema_version": "3.0",
        "dataset_ids": [dataset_id],
        "markets": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        "range": {"start": start, "end": end},
        "strategies": [],
        "policy_selections": policy_refs,
        "causal_execution": "DECLARED_POLICY_WARMUP",
        "decision_interval": "h1",
        "execution_resolution": "h1",
        "latency_ms": 0,
        "initial_cash": "10000000",
        "costs": {
            "buy_fee_bps": "5",
            "sell_fee_bps": "5",
            "maker_fee_bps": "5",
            "half_spread_bps": "0",
            "slippage_bps": "0",
            "impact_bps": "0",
            "assumption_label": scenario,
        },
        "execution": {"kind": "NEXT_BAR_OPEN", "participation_cap": "0.01"},
        "market_rules": {
            "id": "research-e2e-explicit-rules",
            "provenance": "EXPLICIT_SCENARIO",
            "valid_range": {"start": start, "end": end},
            "observed_at": end,
            "source_refs": ["https://docs.upbit.com/kr/docs/krw-market-info"],
            "assumption_label": scenario,
            "min_notional": "5000",
            "quantity_step": "0.00000001",
            "ticks": [{"lower_bound": "0", "tick": "0.00000001"}],
        },
        "terminal_policy": "MARK_TO_MARKET",
        "evidence_snapshot_id": None,
        "pit_policy": "STRICT_PIT",
        "evidence_unavailable": "CASH_WITH_MATCHED_CONTROL",
        "report_clock": {
            "timezone": "UTC",
            "min_annualization_days": 1,
            "risk_free_annual": 0.0,
        },
        "seed": 20261004,
    }


def compare_contract(client, suite_id, expected_phases):
    page = client.call(
        "research_suite",
        {"action": "comparisons", "suite_id": suite_id, "offset": 0, "limit": 100},
    )
    if not page["records"]:
        raise RuntimeError(f"suite {suite_id} produced no comparison rows")
    phases = {row["phase"] for row in page["records"]}
    if not expected_phases.issubset(phases):
        raise RuntimeError(f"suite {suite_id} phases {phases} omit {expected_phases}")
    for row in page["records"]:
        costs = row.get("costs", {})
        comparison = row.get("comparison", {})
        if "scenario_index" not in row or "policy_ref" not in comparison:
            raise RuntimeError("comparison omitted scenario or policy reference")
        for key in ("buy_fee_bps", "sell_fee_bps", "maker_fee_bps", "slippage_bps"):
            if key not in costs:
                raise RuntimeError(f"comparison costs omitted {key}")
        if not comparison.get("semantic_digest") or not comparison.get("causal_input_digest"):
            raise RuntimeError("comparison omitted semantic or causal identity")
    return page


def run(binary, evidence_root):
    evidence_root.mkdir(parents=True, exist_ok=True)
    report_path = evidence_root / "research-mcp-e2e-report.json"
    log_path = evidence_root / "research-mcp-e2e-server.log"
    result = {"status": "FAIL", "binary": str(binary), "steps": {}}
    server = None
    with tempfile.TemporaryDirectory(prefix="sky-backcraft-research-e2e-") as scratch_text:
        scratch = Path(scratch_text)
        root = scratch / "research"
        restored_root = scratch / "restored"
        try:
            server = IsolatedServer(binary, root, log_path)
            client = server.start()
            initialized = client.initialize()
            tools = client.rpc("tools/list")["tools"]
            names = {tool["name"] for tool in tools}
            required = {"research_suite", "collection_schedule", "storage_maintenance"}
            if len(tools) < 20 or not required.issubset(names):
                raise RuntimeError(f"unexpected tool catalog ({len(tools)}): {sorted(names)}")
            status = client.call("lab_status", {})
            source_digest = current_source_digest()
            if status["source_digest"] != source_digest:
                raise RuntimeError(
                    "runtime source digest differs from the current repository: "
                    f"{status['source_digest']} != {source_digest}"
                )
            result.update(
                {
                    "protocol": initialized["protocolVersion"],
                    "tool_count": len(tools),
                    "source_digest": source_digest,
                }
            )
            result["steps"]["transport"] = "PASS"

            now = datetime.now(timezone.utc)
            end_dt = now.replace(minute=0, second=0, microsecond=0)
            start_dt = end_dt - timedelta(hours=72)
            stamp = now.strftime("%Y%m%d%H%M%S%f")
            start = start_dt.isoformat().replace("+00:00", "Z")
            end = end_dt.isoformat().replace("+00:00", "Z")
            collect = submit_and_wait(
                client,
                "collect_data",
                {
                    "request_id": f"research-e2e-collect-{stamp}",
                    "markets": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
                    "range": {"start": start, "end": end},
                    "data_resolution": "h1",
                    "warmup_bars": 8,
                    "completed_only": True,
                },
            )
            dataset_id = output_value(collect, "DATASET", "dataset_id")
            result["dataset_id"] = dataset_id
            result["steps"]["public_upbit_collection"] = "PASS"

            refs = [
                revised_policy(
                    client,
                    "builtin-s1",
                    {
                        "schema_version": "1.0",
                        "name": "E2E S1 short warmup",
                        "description": "Exact S1 candidate for bounded live research E2E.",
                        "program": {
                            "kind": "BUILTIN",
                            "strategy": {
                                "kind": "S1",
                                "state": {"ema_length": 4, "vol_length": 3, "k": 0.5},
                            },
                        },
                    },
                    f"research-e2e-policy-s1-{stamp}",
                ),
                revised_policy(
                    client,
                    "builtin-s2",
                    {
                        "schema_version": "1.0",
                        "name": "E2E S2 short warmup",
                        "description": "Exact S2 candidate for bounded live research E2E.",
                        "program": {
                            "kind": "BUILTIN",
                            "strategy": {"kind": "S2", "entry_length": 4, "exit_length": 3},
                        },
                    },
                    f"research-e2e-policy-s2-{stamp}",
                ),
            ]
            template = experiment(dataset_id, refs, start, end)
            result["policy_refs"] = refs
            result["steps"]["frozen_policy_revisions"] = "PASS"

            batch_request = {
                "request_id": f"research-e2e-batch-{stamp}",
                "template": template,
                "design": {"kind": "batch"},
                "cost_sweep": {"fee_bps": ["2", "5"], "slippage_bps": ["1"]},
            }
            batch_created, batch_done = suite(
                client, {"action": "create", "request": batch_request}
            )
            if batch_done["status"] != "completed":
                raise RuntimeError(f"batch suite blocked: {batch_done}")
            duplicate = client.call("research_suite", {"action": "create", "request": batch_request})
            if duplicate["id"] != batch_created["id"]:
                raise RuntimeError("duplicate suite request changed durable identity")
            compare_contract(client, batch_created["id"], {"batch"})
            result["batch_suite_id"] = batch_created["id"]
            result["steps"]["batch_suite"] = "PASS"

            reused_request = json.loads(json.dumps(batch_request))
            reused_request["request_id"] = f"research-e2e-batch-reuse-{stamp}"
            reused_created, reused_done = suite(
                client, {"action": "create", "request": reused_request}
            )
            if reused_done["status"] != "completed" or reused_created["id"] == batch_created["id"]:
                raise RuntimeError("independent identical suite did not complete with a new identity")
            original_cases = client.call(
                "research_suite",
                {"action": "cases", "suite_id": batch_created["id"], "offset": 0, "limit": 100},
            )
            reused_cases = client.call(
                "research_suite",
                {"action": "cases", "suite_id": reused_created["id"], "offset": 0, "limit": 100},
            )
            original_plans = {case["plan_id"] for case in original_cases["records"]}
            reused_plans = {case["plan_id"] for case in reused_cases["records"]}
            if original_plans != reused_plans or None in original_plans:
                raise RuntimeError("identical suite specification did not reuse frozen plan identities")
            result["steps"]["identical_spec_reuse"] = "PASS"

            walk_request = {
                "request_id": f"research-e2e-walk-{stamp}",
                "template": template,
                "design": {
                    "kind": "walk_forward",
                    "selection_bars": 24,
                    "evaluation_bars": 12,
                    "step_bars": 36,
                    "embargo_bars": 0,
                },
                "cost_sweep": {"fee_bps": ["2", "5"], "slippage_bps": ["1"]},
            }
            walk_created = client.call(
                "research_suite", {"action": "create", "request": walk_request}
            )
            walk_id = walk_created["id"]

            def progress_before_pause():
                current = client.call("research_suite", {"action": "get", "suite_id": walk_id})
                if 0 < current["completed_runs"] < current["planned_runs"]:
                    return current
                if current["status"] != "running":
                    raise RuntimeError(f"walk-forward reached {current['status']} before pause proof")
                return None

            before_pause = wait_for("walk-forward partial progress", progress_before_pause, 120, 0.1)
            paused = client.call("research_suite", {"action": "pause", "suite_id": walk_id})
            if paused["status"] != "paused":
                raise RuntimeError("walk-forward pause was not durable")
            completed_before_restart = before_pause["completed_runs"]
            cases_before = client.call(
                "research_suite",
                {"action": "cases", "suite_id": walk_id, "offset": 0, "limit": 100},
            )
            completed_ids = {
                item["id"] for item in cases_before["records"] if item["status"] == "completed"
            }
            server.stop()
            client = server.start()
            after_restart = client.call("research_suite", {"action": "get", "suite_id": walk_id})
            if after_restart["status"] != "paused" or after_restart["completed_runs"] < completed_before_restart:
                raise RuntimeError("restart lost paused suite progress")
            client.call("research_suite", {"action": "resume", "suite_id": walk_id})
            _, walk_done = suite(client, {"action": "get", "suite_id": walk_id})
            if walk_done["status"] != "completed" or len(walk_done["selected_folds"]) != 2:
                raise RuntimeError(f"walk-forward did not complete two folds: {walk_done}")
            cases_after = client.call(
                "research_suite",
                {"action": "cases", "suite_id": walk_id, "offset": 0, "limit": 100},
            )
            if not completed_ids.issubset({item["id"] for item in cases_after["records"]}):
                raise RuntimeError("restart/resume replaced a completed suite case")
            comparisons = compare_contract(client, walk_id, {"selection", "evaluation"})
            result["walk_forward_suite_id"] = walk_id
            result["walk_forward_comparison_count"] = comparisons["total_count"]
            result["steps"]["walk_forward_restart_resume"] = "PASS"

            evaluation = next(
                row for row in comparisons["records"] if row["phase"] == "evaluation"
            )
            exported = submit_and_wait(
                client,
                "export_report",
                {
                    "request_id": f"research-e2e-export-{stamp}",
                    "run_id": evaluation["comparison"]["run_id"],
                    "market": None,
                },
            )
            manifest = next(
                artifact
                for artifact in exported["artifacts"]
                if artifact["file_name"] == "manifest.json"
            )
            verified = submit_and_wait(
                client,
                "verify_run",
                {
                    "request_id": f"research-e2e-verify-{stamp}",
                    "artifact_id": manifest["artifact_id"],
                    "replay": True,
                },
            )
            validation = latest_attempt(verified)["state"]["output"]
            if validation.get("kind") != "VALIDATION" or validation["report"]["status"] != "PASS":
                raise RuntimeError(f"export verification did not pass: {validation}")
            result["verified_run_id"] = evaluation["comparison"]["run_id"]
            result["steps"]["export_replay_verify"] = "PASS"

            schedule = client.call(
                "collection_schedule",
                {
                    "action": "create",
                    "request": {
                        "request_id": f"research-e2e-schedule-{stamp}",
                        "markets": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
                        "interval": "h1",
                        "lookback_bars": 72,
                        "cadence_seconds": 60,
                        "retry": {"max_retries": 2, "backoff_seconds": 60},
                    },
                },
            )
            schedule_id = schedule["id"]

            def schedule_success():
                current = client.call(
                    "collection_schedule", {"action": "get", "schedule_id": schedule_id}
                )
                if current.get("last_success_dataset_id"):
                    return current
                if current["status"] == "blocked":
                    raise RuntimeError(f"collection schedule blocked: {current}")
                return None

            schedule_done = wait_for("scheduled collection", schedule_success, 180, 0.2)
            boundary = schedule_done["last_success_boundary"]
            schedule_dataset = schedule_done["last_success_dataset_id"]
            schedule_job = (schedule_done.get("in_flight") or {}).get("job_id")
            freshness = client.call(
                "collection_schedule", {"action": "freshness", "schedule_id": schedule_id}
            )
            if len(freshness["markets"]) != 3 or any(
                market["latest_completed_end"] is None for market in freshness["markets"]
            ):
                raise RuntimeError(f"schedule freshness is incomplete: {freshness}")
            server.stop()
            client = server.start()
            schedule_after_restart = client.call(
                "collection_schedule", {"action": "get", "schedule_id": schedule_id}
            )
            if (
                schedule_after_restart["last_success_boundary"] != boundary
                or schedule_after_restart["last_success_dataset_id"] != schedule_dataset
                or (schedule_after_restart.get("in_flight") or {}).get("job_id") != schedule_job
            ):
                raise RuntimeError("restart duplicated or replaced the completed schedule boundary")
            paused_schedule = client.call(
                "collection_schedule", {"action": "pause", "schedule_id": schedule_id}
            )
            if paused_schedule["status"] != "paused":
                raise RuntimeError("schedule pause was not durable")
            result["schedule_id"] = schedule_id
            result["steps"]["schedule_restart_freshness"] = "PASS"

            before_usage = client.call("storage_maintenance", {"action": "usage"})
            backup_request = f"research-e2e-backup-{stamp}"
            backup_first = client.call(
                "storage_maintenance",
                {"action": "create_backup", "request_id": backup_request},
            )["receipt"]
            backup_again = client.call(
                "storage_maintenance",
                {"action": "create_backup", "request_id": backup_request},
            )["receipt"]
            if backup_first != backup_again:
                raise RuntimeError("managed backup request was not idempotent")
            retention = client.call(
                "storage_maintenance",
                {
                    "action": "retention_candidates",
                    "cutoff": "2999-01-01T00:00:00Z",
                    "keep_recent": 1,
                    "offset": 0,
                    "limit": 100,
                },
            )
            client.call("research_suite", {"action": "get", "suite_id": walk_id})
            client.call("collection_schedule", {"action": "get", "schedule_id": schedule_id})
            after_usage = client.call("storage_maintenance", {"action": "usage"})
            if after_usage["usage"]["managed_backup_count"] != before_usage["usage"]["managed_backup_count"] + 1:
                raise RuntimeError("maintenance accounting did not reflect exactly one backup")
            result["retention_candidate_count"] = retention["page"]["total_count"]
            result["backup_receipt"] = backup_first
            result["steps"]["backup_and_retention_preview"] = "PASS"

            server.stop()
            backup_matches = list(scratch.glob(f".sky-backcraft-managed-backups-*/{backup_first['id']}"))
            if len(backup_matches) != 1:
                raise RuntimeError(f"cannot resolve managed backup directory: {backup_matches}")
            restore = subprocess.run(
                [
                    str(binary),
                    "restore",
                    "--backup",
                    str(backup_matches[0]),
                    "--data-root",
                    str(restored_root),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
                check=False,
            )
            if restore.returncode != 0:
                raise RuntimeError(f"restore failed: {restore.stderr[-2000:]}")
            restored_server = IsolatedServer(
                binary,
                restored_root,
                evidence_root / "research-mcp-e2e-restored.log",
                public_no_auth=False,
            )
            server = restored_server
            restored_client = server.start()
            restored_suite = restored_client.call(
                "research_suite", {"action": "get", "suite_id": walk_id}
            )
            restored_schedule = restored_client.call(
                "collection_schedule", {"action": "get", "schedule_id": schedule_id}
            )
            restored_backups = restored_client.call(
                "storage_maintenance", {"action": "usage"}
            )
            if restored_suite["status"] != "completed" or restored_schedule["status"] != "paused":
                raise RuntimeError("restored research state differs from the verified backup")
            if restored_backups["usage"]["sqlite"]["allocated_bytes"] == 0:
                raise RuntimeError("restored SQLite accounting is empty")
            result["steps"]["verified_restore"] = "PASS"
            result["status"] = "PASS"
        except Exception as error:
            result["error"] = f"{type(error).__name__}: {error}"
            if log_path.exists():
                result["server_log_tail"] = log_path.read_text(errors="replace")[-4000:]
            raise
        finally:
            if server is not None:
                try:
                    server.stop()
                except Exception as stop_error:
                    result.setdefault("shutdown_error", str(stop_error))
                    if result["status"] == "PASS":
                        result["status"] = "FAIL"
            report_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--evidence-root", required=True, type=Path)
    arguments = parser.parse_args()
    binary = arguments.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"binary is not executable: {binary}")
    result = run(binary, arguments.evidence_root.resolve())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"research MCP E2E failed: {error}", file=sys.stderr)
        raise
