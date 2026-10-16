#!/usr/bin/env python3
"""Exercise real public-candle ingestion and durable paper continuation in a fresh root.

Only anonymous Upbit public candle GETs are allowed. The helper runs with a fake desktop
token and no private exchange credential material. The paper policy is SIMULATED. The
script waits for finalized one-minute bars, pauses the session, restarts the exact helper,
checks durable state and cursor replay, resumes through an overlapping public response,
then leaves the verified session PAUSED for an optional AppKit review.
"""

from __future__ import annotations

import argparse
import datetime as dt
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
import paper_session_recovery_mcp_e2e as support  # noqa: E402


EXPECTED_HELPER_SHA256 = "5b6f99aad57f296c68d9fe4d8c658b56276ecf4f6f5db5ab5fe5b2c428744f20"
DEFAULT_HELPER = support.DEFAULT_BINARY
DEFAULT_APP = support.DEFAULT_APP_EXECUTABLE
FORBIDDEN_TOOLS = {
    "upbit_account_balances",
    "upbit_orders_open",
    "upbit_orders_closed",
    "upbit_order_get",
    "upbit_order_chance",
    "upbit_order_test",
    "upbit_order_execute",
    "upbit_order_cancel",
    "trading_control",
}


class FeedHarnessError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def require_check(checks: list[dict[str, Any]], name: str, passed: bool, details: Any = None) -> None:
    checks.append({"name": name, "result": "PASS" if passed else "FAIL", "details": details})
    if not passed:
        raise FeedHarnessError(f"check failed: {name}: {details}")


def session_facts(session: dict[str, Any]) -> dict[str, Any]:
    market_fields = (
        "market", "model_id", "initial_cash", "cash_total", "cash_free", "cash_reserved",
        "quantity", "mark_price", "equity", "realized_pnl", "unrealized_pnl", "total_fees",
        "drawdown_amount", "drawdown_fraction", "signal_count", "fill_count", "event_count",
        "last_source_event_at", "last_finalized_candle_at", "last_account_mark_at",
        "last_heartbeat_at", "freshness", "verification",
    )
    return {
        key: session.get(key)
        for key in (
            "session_id", "origin_request_id", "state", "sequence", "policy_ref", "mode",
            "decision_interval", "execution_resolution", "created_at", "started_at",
            "paused_at", "stopped_at", "last_evaluated_at", "event_count",
        )
    } | {
        "markets": [
            {key: market.get(key) for key in market_fields}
            for market in session.get("markets", [])
        ]
    }


def stable_checkpoint_projection(session: dict[str, Any]) -> dict[str, Any]:
    """Compare persisted economic fields across restart, excluding time-relative freshness."""
    facts = session_facts(session)
    for market in facts["markets"]:
        market.pop("freshness", None)
    facts.pop("last_evaluated_at", None)
    return facts


def candle_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [event for event in events if event.get("kind") == "CANDLE_PROCESSED"]


def validate_candle_history(events: list[dict[str, Any]], expected_interval: str) -> dict[str, Any]:
    candles = candle_events(events)
    ids = [event.get("candle", {}).get("observation_id") for event in candles]
    closes = [event.get("candle", {}).get("close_time") for event in candles]
    opens = [event.get("candle", {}).get("open_time") for event in candles]
    if any(not isinstance(value, str) or not value for value in ids):
        raise FeedHarnessError("processed candle event omitted observation_id")
    if len(ids) != len(set(ids)):
        raise FeedHarnessError("paper event history repeated a candle observation ID")
    if any((event.get("candle") or {}).get("interval") != expected_interval for event in candles):
        raise FeedHarnessError("processed candle event interval differs from the selected interval")
    gaps = [
        {"previous_close": closes[index - 1], "next_open": opens[index]}
        for index in range(1, len(candles))
        if closes[index - 1] != opens[index]
    ]
    return {
        "processed_candle_event_count": len(candles),
        "unique_observation_ids": len(set(ids)),
        "duplicate_observation_ids": len(ids) - len(set(ids)),
        "close_open_gaps": gaps,
        "continuous": not gaps,
        "events": support.event_summaries(events),
    }


def validate_causal_provenance(events: list[dict[str, Any]], session: dict[str, Any]) -> dict[str, Any]:
    started_at = dt.datetime.fromisoformat(session["started_at"].replace("Z", "+00:00"))
    candles = candle_events(events)
    now = dt.datetime.now(dt.timezone.utc)
    event_checks = []
    for event in candles:
        candle = event.get("candle") or {}
        close_at = dt.datetime.fromisoformat(candle["close_time"].replace("Z", "+00:00"))
        finalized_at = dt.datetime.fromisoformat(event["finalized_at"].replace("Z", "+00:00"))
        source_at = dt.datetime.fromisoformat(event["source_event_at"].replace("Z", "+00:00"))
        event_checks.append({
            "sequence": event.get("sequence"),
            "close_after_session_start": close_at > started_at,
            "finalized_matches_candle_close": finalized_at == close_at,
            "finalized_before_source_observation": finalized_at <= source_at,
            "source_observation_not_future": source_at <= now,
        })
    if not event_checks or not all(
        check["close_after_session_start"]
        and check["finalized_matches_candle_close"]
        and check["finalized_before_source_observation"]
        and check["source_observation_not_future"]
        for check in event_checks
    ):
        raise FeedHarnessError(f"paper candle provenance failed causal checks: {event_checks}")
    latest = candles[-1]
    market = (session.get("markets") or [{}])[0]
    expected = {
        "last_source_event_at": latest.get("source_event_at"),
        "last_finalized_candle_at": latest.get("finalized_at"),
        "last_heartbeat_at": latest.get("source_event_at"),
    }
    matched = all(market.get(key) == value for key, value in expected.items())
    if not matched:
        raise FeedHarnessError(f"session source/finalized/heartbeat projection differs from last event: {expected} vs {market}")
    return {"event_checks": event_checks, "session_markers_match_latest_event": matched}


def verify_exact_virtual_economics(events: list[dict[str, Any]], session: dict[str, Any]) -> dict[str, Any]:
    market = (session.get("markets") or [{}])[0]
    fills = [fill for event in events for fill in event.get("fills", [])]
    marks = [mark for event in events for mark in event.get("account_marks", [])]
    if not fills:
        raise FeedHarnessError("SIMULATED paper feed produced no virtual fills within the bounded bar window")
    fill_ids = [fill.get("fill_id") for fill in fills]
    if len(fill_ids) != len(set(fill_ids)) or any(not fill_id for fill_id in fill_ids):
        raise FeedHarnessError("virtual fill IDs are missing or duplicated")
    fee_total = sum((Decimal(fill["fee"]) for fill in fills), Decimal(0))
    net_quantity = sum(
        (Decimal(fill["qty"]) if fill["side"].upper() == "BUY" else -Decimal(fill["qty"]) for fill in fills),
        Decimal(0),
    )
    if Decimal(market["total_fees"]) != fee_total:
        raise FeedHarnessError(f"durable total_fees {market['total_fees']} != exact fill fee sum {fee_total}")
    if Decimal(market["quantity"]) != net_quantity:
        raise FeedHarnessError(f"durable quantity {market['quantity']} != exact fill quantity sum {net_quantity}")
    if int(market.get("fill_count", 0)) != len(fills):
        raise FeedHarnessError(f"durable fill_count {market.get('fill_count')} != event fill count {len(fills)}")
    if not marks:
        raise FeedHarnessError("processed paper events contain no durable account marks")
    last_mark = marks[-1]
    if market.get("equity") is None or Decimal(market["equity"]) != Decimal(last_mark["equity"]):
        raise FeedHarnessError(f"durable equity {market.get('equity')} != last event mark {last_mark.get('equity')}")
    return {
        "fill_count": len(fills),
        "fills": fills,
        "exact_fill_fee_sum": str(fee_total),
        "exact_net_fill_quantity": str(net_quantity),
        "durable_total_fees": market["total_fees"],
        "durable_quantity": market["quantity"],
        "last_account_mark": last_mark,
        "durable_equity": market["equity"],
        "fee_quantity_and_equity_match": True,
    }


def raw_response_summaries(data_root: Path) -> list[dict[str, Any]]:
    raw_root = data_root / "raw"
    if not raw_root.is_dir():
        return []
    summaries = []
    for path in sorted(raw_root.rglob("body.json.gz")):
        try:
            compressed = path.read_bytes()
            body = gzip.decompress(compressed)
            payload = json.loads(body)
        except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as error:
            raise FeedHarnessError(f"cannot inspect owned public raw response {path}: {error}") from error
        rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        stamps = sorted(
            row["candle_date_time_utc"]
            for row in rows
            if isinstance(row.get("candle_date_time_utc"), str)
        )
        summaries.append({
            "relative_path": str(path.relative_to(data_root)),
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "row_count": len(rows),
            "markets": sorted({row.get("market") for row in rows if row.get("market")}),
            "oldest_candle_open_utc": stamps[0] if stamps else None,
            "newest_candle_open_utc": stamps[-1] if stamps else None,
        })
    return summaries


def overlap_for_roots(data_root: Path, before: list[dict[str, Any]], after: list[dict[str, Any]]) -> dict[str, Any]:
    def opens_for(item: dict[str, Any]) -> set[str]:
        path = data_root / item["relative_path"]
        with gzip.open(path, "rt", encoding="utf-8") as source:
            payload = json.load(source)
        return {
            row["candle_date_time_utc"]
            for row in payload
            if isinstance(row, dict) and isinstance(row.get("candle_date_time_utc"), str)
        }

    before_paths = {item["relative_path"] for item in before}
    new_after = [item for item in after if item["relative_path"] not in before_paths]
    before_sets = [opens_for(item) for item in before]
    after_sets = [opens_for(item) for item in new_after]
    overlaps = [len(old & new) for old in before_sets for new in after_sets if old and new]
    return {
        "compared_body_pairs": len(overlaps),
        "new_response_body_count": len(new_after),
        "pairs_with_repeated_candle_opens": sum(count > 0 for count in overlaps),
        "max_repeated_candle_opens_in_pair": max(overlaps, default=0),
        "overlap_confirmed": bool(new_after) and any(count > 0 for count in overlaps),
    }


def make_start_request(options: dict[str, Any], request_id: str, interval: str) -> dict[str, Any]:
    supported = [option for option in options.get("policy_options", []) if option.get("supported") is True]
    if not supported:
        raise FeedHarnessError("paper start_options has no supported policy")
    policy = next(
        (option for option in supported if option.get("reference", {}).get("policy_id") == "builtin-buy-and-hold"),
        supported[0],
    )
    presets = options.get("scenario_presets", [])
    if not presets:
        raise FeedHarnessError("paper start_options has no scenario preset")
    preset = presets[0]
    virtual_cash: str | int = "10000000" if isinstance(preset["initial_cash"], str) else 10_000_000
    return {
        "action": "start",
        "request_id": request_id,
        "policy_ref": policy["reference"],
        "decision_interval": interval,
        "execution_resolution": interval,
        "latency_ms": preset["latency_ms"],
        "execution": preset["execution"],
        "markets": [{
            "market": "KRW-BTC",
            # Virtual funds only; this funds a 0.01 BTC lot under the supported
            # scenario presets without contacting any execution endpoint.
            "initial_cash": virtual_cash,
            "costs": preset["costs"],
            "market_rules": preset["market_rules"],
        }],
    }


def expected_helper_command(binary: Path, port: int, data_root: Path) -> list[str]:
    return [str(binary), "mcp-serve", "--bind", "127.0.0.1", "--port", str(port), "--data-root", str(data_root), "--public-no-auth"]


def assert_process_and_listener(pid: int, command: list[str], port: int) -> None:
    actual = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", "command="], capture_output=True, text=True).stdout.strip()
    if actual != " ".join(command):
        raise FeedHarnessError(f"helper command identity changed for PID {pid}: {actual!r}")
    listeners = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"], capture_output=True, text=True).stdout
    if str(pid) not in listeners or str(port) not in listeners:
        raise FeedHarnessError(f"owned helper PID {pid} is not listening on 127.0.0.1:{port}")


def wait_for_events(
    client: support.McpClient,
    session_id: str,
    *,
    minimum_candles: int,
    minimum_fills: int = 0,
    timeout_seconds: int,
    after_sequence: int = 0,
    label: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout_seconds
    next_report = time.monotonic()
    last_session: dict[str, Any] = {}
    last_events: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last_session = support.query_session(client, "get", session_id)
        last_events = support.read_events(client, session_id, after_sequence=after_sequence, limit=50)
        new_candles = [event for event in last_events if event.get("kind") == "CANDLE_PROCESSED"]
        market = (last_session.get("markets") or [{}])[0]
        if len(new_candles) >= minimum_candles and int(market.get("fill_count", 0)) >= minimum_fills:
            return last_session, last_events
        if any(event.get("kind") == "RECOVERY_REQUIRED" for event in last_events) or last_session.get("state") == "RECOVERY_REQUIRED":
            raise FeedHarnessError(f"paper session entered RECOVERY_REQUIRED while waiting for {label}")
        if last_session.get("state") != "RUNNING":
            raise FeedHarnessError(f"paper session left RUNNING while waiting for {label}: {last_session.get('state')}")
        if time.monotonic() >= next_report:
            print(json.dumps({
                "progress": label,
                "session_state": last_session.get("state"),
                "sequence": last_session.get("sequence"),
                "candle_events_seen": len(new_candles),
                "fill_count": market.get("fill_count"),
                "freshness": market.get("freshness"),
                "last_finalized_candle_at": market.get("last_finalized_candle_at"),
                "deadline_seconds": max(0, round(deadline - time.monotonic())),
            }, ensure_ascii=False), flush=True)
            next_report = time.monotonic() + 15
        time.sleep(5)
    return last_session, last_events


def write_with_readback(
    client: support.McpClient,
    request_id: str,
    arguments: dict[str, Any],
    *,
    timeout: float = 90,
) -> dict[str, Any]:
    try:
        response = client.call("paper_session_write", arguments, timeout=timeout)
        response["_resolution"] = "DIRECT_RESPONSE"
        return response
    except Exception as error:
        readback = client.call("paper_session_query", {"action": "request", "request_id": request_id})
        if readback.get("found") is True and isinstance(readback.get("receipt"), dict) and isinstance(readback.get("session"), dict):
            return {"receipt": readback["receipt"], "session": readback["session"], "_resolution": "DURABLE_REQUEST_READBACK"}
        raise FeedHarnessError(f"request {request_id} uncertain and not durably found; no resend: {error}") from error


def public_probe_diagnostic(
    desktop_client: support.McpClient,
    port: int,
    token: str,
    tool_log: list[dict[str, Any]],
) -> dict[str, Any]:
    """One bounded anonymous public read to expose an exact poll error if paper stalled."""
    try:
        tools = desktop_client.rpc("tools/list").get("tools", [])
        public_client = desktop_client
        if "probe_upbit" not in {item.get("name") for item in tools}:
            public_client = support.McpClient(port, token, tool_log)
            public_client.url = f"http://127.0.0.1:{port}/mcp"
            public_client.initialize()
            public_tools = public_client.rpc("tools/list").get("tools", [])
            if "probe_upbit" not in {item.get("name") for item in public_tools}:
                return {"available": False, "reason": "probe_upbit not exposed on desktop or public MCP"}
        result = public_client.call(
            "probe_upbit",
            {"market": "KRW-BTC", "interval": "m1", "count": 1, "save_raw": False},
            allow_error=True,
            timeout=25,
        )
        if result.get("is_error") is True:
            return {"available": True, "is_error": True, "exact_error": result.get("text")}
        return {"available": True, "is_error": False, "probe_report": result}
    except Exception as error:
        return {"available": False, "exact_error": f"{type(error).__name__}: {error}"}


def create_runtime(
    *,
    binary: Path,
    root: Path,
    data_root: Path,
    port: int,
    token: str,
    env: dict[str, str],
    log_path: Path,
    tool_call_log: list[dict[str, Any]],
) -> tuple[subprocess.Popen[bytes], support.McpClient, list[str]]:
    command = expected_helper_command(binary, port, data_root)
    with log_path.open("xb", buffering=0) as log:
        process = subprocess.Popen(
            command,
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        client = support.wait_for_ready(process, port, token, tool_call_log)
    except Exception:
        # This process was created here; if readiness fails it is the only PID touched.
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pass
        raise
    return process, client, command


def stable_event_check(events: list[dict[str, Any]], interval: str) -> dict[str, Any]:
    result = validate_candle_history(events, interval)
    if not result["continuous"]:
        raise FeedHarnessError(f"finalized public candle history has gaps: {result['close_open_gaps']}")
    return result


def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        json.dump(receipt, output, indent=2, ensure_ascii=False, sort_keys=True)
        output.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_HELPER)
    parser.add_argument("--app-executable", type=Path, default=DEFAULT_APP)
    parser.add_argument("--evidence-root", type=Path, default=Path(".omx/evidence/p13/paper-public-feed"))
    parser.add_argument("--initial-candle-timeout", type=int, default=300)
    parser.add_argument("--resume-candle-timeout", type=int, default=150)
    parser.add_argument("--initial-candles", type=int, default=2)
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parents[1]
    binary = args.binary.resolve(strict=True)
    if binary.is_symlink() or not os.access(binary, os.X_OK):
        parser.error("--binary must be a regular executable release helper")
    binary_sha256 = support.sha256_file(binary)
    if binary_sha256 != EXPECTED_HELPER_SHA256:
        parser.error(f"release helper SHA mismatch: {binary_sha256}")
    if sys.platform != "darwin":
        parser.error("the packaged AppKit paper review harness requires macOS")
    if not os.environ.get("HOME"):
        parser.error("HOME must be preserved from the existing user environment")
    app_executable = args.app_executable.resolve(strict=True)
    if not app_executable.is_file() or not os.access(app_executable, os.X_OK):
        parser.error("--app-executable must be the existing packaged review executable")

    evidence_root = args.evidence_root if args.evidence_root.is_absolute() else workspace / args.evidence_root
    evidence_root.mkdir(parents=True, exist_ok=True)
    if evidence_root.is_symlink():
        parser.error("evidence root must not be a symlink")
    run_id = uuid.uuid4().hex
    evidence_dir = evidence_root / f"public-feed-{run_id[:12]}"
    evidence_dir.mkdir(mode=0o755, exist_ok=False)
    root = support.make_root()
    data_root = root / "research"
    port = support.allocate_loopback_port()
    if port == 8130:
        raise FeedHarnessError("ephemeral loopback allocator returned protected port 8130")
    token = uuid.uuid4().hex * 2
    settings_path = support.write_review_config(root, port)
    app_sha256 = support.sha256_file(app_executable)
    script_sha256 = support.sha256_file(Path(__file__).resolve())
    request_ids = {
        name: f"p13-public-feed-{run_id}-{name}"
        for name in ("start", "pause-before-restart", "resume-after-restart", "pause-for-handoff")
    }
    checks: list[dict[str, Any]] = []
    tool_call_log: list[dict[str, Any]] = []
    process_history: list[dict[str, Any]] = []
    commands: list[list[str]] = []
    client: support.McpClient | None = None
    process: subprocess.Popen[bytes] | None = None
    session_id: str | None = None
    handoff_ready = False
    failure: str | None = None
    intervals_seen: list[str] = []
    source_digest = support.current_source_digest(workspace)

    env = support.sanitized_environment(token, root)
    credential_names = sorted(
        name for name in env
        if name in {
            "SERVICE_UPBIT_PRIVATE_ACCESS_KEY",
            "SERVICE_UPBIT_PRIVATE_SECRET_KEY",
            "SERVICE_UPBIT_CREDENTIAL_FILE",
        }
    )
    require_check(
        checks,
        "sanitized helper environment excludes private credential inputs",
        not credential_names,
        {"credential_keys_present": credential_names, "environment_keys": sorted(env)},
    )
    require_check(
        checks,
        "fresh runtime root has no dotenv credential file",
        not (root / ".env").exists() and not (data_root / ".env").exists(),
        {"root_dotenv": (root / ".env").exists(), "data_root_dotenv": (data_root / ".env").exists()},
    )

    run: dict[str, Any] = {
        "created_at_utc": utc_now(),
        "input_revision": subprocess.run(["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True, text=True).stdout.strip(),
        "helper_binary": str(binary),
        "helper_binary_sha256": binary_sha256,
        "script_sha256": script_sha256,
        "current_rust_source_digest": source_digest,
        "temporary_root": str(root),
        "data_root": str(data_root),
        "settings_path": str(settings_path),
        "settings": {"domain": "", "port": port, "authenticationMode": "none"},
        "port": port,
        "desktop_endpoint": f"http://127.0.0.1:{port}/desktop/mcp",
        "desktop_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "app_executable": str(app_executable),
        "app_sha256": app_sha256,
        "recommended_app_arguments": ["--gui-review"],
        "do_not_add_argument": "--gui-review-read-only enables live account reads in this app revision",
        "public_source": "fixed anonymous Upbit quotation endpoint used by the pinned helper",
        "private_exchange_credentials_inherited": False,
        "private_exchange_tools_called": False,
        "real_orders_cancel_transfers_or_withdrawals_called": False,
        "mcp_tool_call_log": tool_call_log,
        "request_ids": request_ids,
        "checks": checks,
        "process_history": process_history,
        "helper_commands": commands,
        "whole_p13_acceptance_claimed": False,
        "fault_injection": {
            "synthetic_feed_used": False,
            "duplicate_input_deliberately_injected": False,
            "missing_bar_deliberately_injected": False,
            "429_deliberately_provoked": False,
            "lag_deliberately_injected": False,
        },
    }

    try:
        process, client, command = create_runtime(
            binary=binary,
            root=root,
            data_root=data_root,
            port=port,
            token=token,
            env=env,
            log_path=evidence_dir / "paper-public-feed-process-1.log",
            tool_call_log=tool_call_log,
        )
        commands.append(command)
        assert_process_and_listener(process.pid, command, port)
        status = client.call("lab_status", {})
        require_check(checks, "helper release identity and current source digest", status.get("source_digest") == source_digest, {
            "helper_source_digest": status.get("source_digest"), "workspace_source_digest": source_digest,
        })
        require_check(checks, "private venue remains MOCK", status.get("scope", {}).get("upbit_private_mode") == "MOCK", status.get("scope"))
        first_log = (evidence_dir / "paper-public-feed-process-1.log").read_text(errors="replace")
        require_check(checks, "no-credential MOCK startup evidence", "Upbit credentials are absent; using explicit MOCK read mode" in first_log, None)

        options = client.call("paper_session_query", {"action": "start_options"})
        intervals_seen = sorted({str(value).lower() for value in options.get("decision_intervals", [])})
        resolutions_seen = sorted({str(value).lower() for value in options.get("execution_resolutions", [])})
        require_check(checks, "M1 supported for decision and execution", "m1" in intervals_seen and "m1" in resolutions_seen, {
            "decision_intervals": intervals_seen, "execution_resolutions": resolutions_seen,
        })

        start_request = make_start_request(options, request_ids["start"], "m1")
        run["start_request_virtual_cash_krw"] = start_request["markets"][0]["initial_cash"]
        start = write_with_readback(client, request_ids["start"], start_request)
        start_session = start.get("session", {})
        session_id = start_session.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            raise FeedHarnessError("SIMULATED paper start produced no durable session_id")
        require_check(checks, "supported M1 SIMULATED paper session started", start.get("receipt", {}).get("outcome") == "APPLIED"
            and start_session.get("mode") == "SIMULATED" and start_session.get("state") == "RUNNING"
            and start_session.get("decision_interval") == "m1" and start_session.get("execution_resolution") == "m1", session_facts(start_session))
        run["policy_ref"] = start_session.get("policy_ref")
        run["session_id"] = session_id
        run["start_session"] = session_facts(start_session)
        run["start_request_readback"] = client.call("paper_session_query", {"action": "request", "request_id": request_ids["start"]})

        print(json.dumps({
            "progress": "waiting_for_public_finalized_m1_bars",
            "session_id": session_id,
            "started_at": start_session.get("started_at"),
            "deadline_seconds": args.initial_candle_timeout,
            "required_candle_events": args.initial_candles,
            "required_virtual_fills": 1,
        }, ensure_ascii=False), flush=True)
        running_session, events = wait_for_events(
            client,
            session_id,
            minimum_candles=args.initial_candles,
            minimum_fills=1,
            timeout_seconds=args.initial_candle_timeout,
            label="initial public M1 paper feed",
        )
        run["initial_running_session"] = session_facts(running_session)
        run["initial_events"] = events
        run["raw_responses_before_restart"] = raw_response_summaries(data_root)
        initial_fills = sum(len(event.get("fills", [])) for event in events)
        if initial_fills < 1:
            run["public_probe_diagnostic"] = public_probe_diagnostic(client, port, token, tool_call_log)
            raise FeedHarnessError(
                "bounded M1 paper polling did not produce one virtual fill; exact public probe diagnostic is recorded"
            )
        initial_history = stable_event_check(events, "m1")
        require_check(checks, "initial public M1 candles are finalized, unique, and gap-free", initial_history["continuous"]
            and initial_history["processed_candle_event_count"] >= args.initial_candles, initial_history)
        initial_provenance = validate_causal_provenance(events, running_session)
        require_check(checks, "initial candle source/finalized timestamps pass causal and freshness projection checks", True, initial_provenance)
        initial_economics = verify_exact_virtual_economics(events, running_session)
        require_check(checks, "exact virtual fee, fill quantity and account mark match durable paper readback", True, initial_economics)
        initial_market = (running_session.get("markets") or [{}])[0]
        require_check(checks, "typed public source/finalized/heartbeat provenance is present", bool(initial_market.get("last_source_event_at"))
            and bool(initial_market.get("last_finalized_candle_at")) and bool(initial_market.get("last_heartbeat_at")), {
                key: initial_market.get(key) for key in ("last_source_event_at", "last_finalized_candle_at", "last_heartbeat_at", "freshness")
            })
        require_check(checks, "processed events identify source and finalized times", all(
            event.get("source_event_at") and event.get("finalized_at") for event in candle_events(events)
        ), initial_history["events"])
        raw_before_restart = run["raw_responses_before_restart"]
        require_check(checks, "paper polling persisted at least one public response body", bool(raw_before_restart), raw_before_restart)
        run["raw_responses_before_restart"] = raw_before_restart
        run["initial_causal_provenance"] = initial_provenance
        run["initial_virtual_economics"] = initial_economics

        pause_before_args = {
            "action": "pause",
            "request_id": request_ids["pause-before-restart"],
            "session_id": session_id,
            "expected_sequence": running_session["sequence"],
        }
        pause_before = write_with_readback(client, request_ids["pause-before-restart"], pause_before_args)
        paused_before = pause_before.get("session", {})
        require_check(checks, "session paused before process restart", pause_before.get("receipt", {}).get("outcome") == "APPLIED"
            and paused_before.get("state") == "PAUSED", session_facts(paused_before))
        pre_restart_events = support.read_events(client, session_id, after_sequence=0, limit=50)
        pre_restart_tail = pre_restart_events[-1]["sequence"] if pre_restart_events else 0
        run["pre_restart_paused_session"] = session_facts(paused_before)
        run["pre_restart_events"] = pre_restart_events
        run["pause_before_restart_readback"] = client.call("paper_session_query", {"action": "request", "request_id": request_ids["pause-before-restart"]})

        first_process = process
        process = None
        stopped_first = support.stop_owned_process(first_process, "pre-recovery public-feed helper")
        process_history.append(stopped_first)

        process, client, command = create_runtime(
            binary=binary,
            root=root,
            data_root=data_root,
            port=port,
            token=token,
            env=env,
            log_path=evidence_dir / "paper-public-feed-process-2-restarted.log",
            tool_call_log=tool_call_log,
        )
        commands.append(command)
        assert_process_and_listener(process.pid, command, port)
        restart_status = client.call("lab_status", {})
        require_check(checks, "restarted helper has same source identity and MOCK mode", restart_status.get("source_digest") == source_digest
            and restart_status.get("scope", {}).get("upbit_private_mode") == "MOCK", {
                "helper_source_digest": restart_status.get("source_digest"), "private_mode": restart_status.get("scope", {}).get("upbit_private_mode"),
            })
        restarted_session = support.query_session(client, "get", session_id)
        restarted_events = support.read_events(client, session_id, after_sequence=0, limit=50)
        require_check(checks, "paused session and durable event tail survive helper restart", restarted_session.get("state") == "PAUSED"
            and restarted_session.get("sequence") == paused_before.get("sequence") and restarted_events == pre_restart_events, {
                "before": session_facts(paused_before), "after": session_facts(restarted_session),
                "event_sequences": [event.get("sequence") for event in restarted_events],
            })
        require_check(checks, "checkpoint economics and durable marks are unchanged across restart", stable_checkpoint_projection(restarted_session)
            == stable_checkpoint_projection(paused_before), {
                "before": stable_checkpoint_projection(paused_before),
                "after": stable_checkpoint_projection(restarted_session),
            })
        cursor_empty = client.call("paper_session_query", {
            "action": "events", "session_id": session_id, "after_sequence": pre_restart_tail, "limit": 1,
        })
        require_check(checks, "event cursor after restart is empty at acknowledged sequence", cursor_empty.get("items") == []
            and cursor_empty.get("returned_count") == 0, cursor_empty)
        start_readback_after_restart = client.call("paper_session_query", {"action": "request", "request_id": request_ids["start"]})
        pause_readback_after_restart = client.call("paper_session_query", {"action": "request", "request_id": request_ids["pause-before-restart"]})
        require_check(checks, "start and pause request receipts survive helper restart", start_readback_after_restart.get("found") is True
            and pause_readback_after_restart.get("found") is True, {
                "start_found": start_readback_after_restart.get("found"),
                "pause_found": pause_readback_after_restart.get("found"),
            })
        run["restart_snapshot"] = session_facts(restarted_session)
        run["restart_events"] = restarted_events
        run["restart_cursor_readback"] = cursor_empty

        resume_args = {
            "action": "resume",
            "request_id": request_ids["resume-after-restart"],
            "session_id": session_id,
            "expected_sequence": restarted_session["sequence"],
        }
        resumed_response = write_with_readback(client, request_ids["resume-after-restart"], resume_args)
        resumed = resumed_response.get("session", {})
        require_check(checks, "post-restart resume applies the durable CAS sequence", resumed_response.get("receipt", {}).get("outcome") == "APPLIED"
            and resumed.get("state") == "RUNNING" and resumed_response.get("receipt", {}).get("sequence") == restarted_session["sequence"] + 1, {
                "receipt": resumed_response.get("receipt"), "session": session_facts(resumed),
            })
        resume_receipt = client.call("paper_session_query", {"action": "request", "request_id": request_ids["resume-after-restart"]})
        require_check(checks, "resume request is durably readable", resume_receipt.get("found") is True, resume_receipt)

        continuation_session, continuation_events = wait_for_events(
            client,
            session_id,
            minimum_candles=1,
            timeout_seconds=args.resume_candle_timeout,
            after_sequence=pre_restart_tail,
            label="public M1 continuation after helper restart",
        )
        all_events = support.read_events(client, session_id, after_sequence=0, limit=50)
        continuation_history = stable_event_check(all_events, "m1")
        require_check(checks, "post-restart public feed continues without duplicate IDs or candle gaps", continuation_history["continuous"]
            and continuation_history["processed_candle_event_count"] > initial_history["processed_candle_event_count"], continuation_history)
        require_check(checks, "post-restart events contain no RECOVERY_REQUIRED", not any(event.get("kind") == "RECOVERY_REQUIRED" for event in all_events), {
            "event_kinds": [event.get("kind") for event in all_events],
        })
        raw_after_restart = raw_response_summaries(data_root)
        overlap = overlap_for_roots(data_root, raw_before_restart, raw_after_restart)
        require_check(checks, "new public response overlaps previously observed bars", overlap["overlap_confirmed"], {
            "before_response_count": len(raw_before_restart),
            "after_response_count": len(raw_after_restart),
            "overlap": overlap,
        })
        run["resumed_session"] = session_facts(continuation_session)
        run["resume_events_after_pre_restart_cursor"] = continuation_events
        run["all_events_after_resume"] = all_events
        run["raw_responses_after_restart"] = raw_after_restart
        run["raw_overlap"] = overlap

        pause_final_args = {
            "action": "pause",
            "request_id": request_ids["pause-for-handoff"],
            "session_id": session_id,
            "expected_sequence": continuation_session["sequence"],
        }
        final_pause = write_with_readback(client, request_ids["pause-for-handoff"], pause_final_args)
        final_session = final_pause.get("session", {})
        require_check(checks, "final live-confirmed session is PAUSED for optional Main CUA", final_pause.get("receipt", {}).get("outcome") == "APPLIED"
            and final_session.get("state") == "PAUSED", session_facts(final_session))
        final_readback = client.call("paper_session_query", {"action": "request", "request_id": request_ids["pause-for-handoff"]})
        require_check(checks, "final pause has durable request readback", final_readback.get("found") is True, final_readback)
        final_events = support.read_events(client, session_id, after_sequence=0, limit=50)
        final_history = stable_event_check(final_events, "m1")
        final_provenance = validate_causal_provenance(final_events, final_session)
        final_economics = verify_exact_virtual_economics(final_events, final_session)
        sequences = [event.get("sequence") for event in final_events]
        require_check(checks, "durable lifecycle and candle event sequence remains contiguous", sequences == list(range(1, len(sequences) + 1))
            and sequences[-1] == final_session.get("sequence"), {
                "event_sequences": sequences, "session_sequence": final_session.get("sequence"),
            })
        require_check(checks, "final source/finalized/heartbeat and exact economics remain consistent", True, {
            "causal_provenance": final_provenance,
            "virtual_economics": final_economics,
            "candle_history": final_history,
        })
        sessions_page = client.call("paper_session_query", {"action": "list", "limit": 50})
        states = {item.get("session_id"): item.get("state") for item in sessions_page.get("items", [])}
        require_check(checks, "only this intended session exists and no session is RUNNING", states == {session_id: "PAUSED"}, states)
        invoked = {entry["name"] for entry in tool_call_log}
        require_check(checks, "no private account/order/control MCP tool was invoked", not (invoked & FORBIDDEN_TOOLS), {
            "invoked_tools": sorted(invoked), "forbidden_tools_invoked": sorted(invoked & FORBIDDEN_TOOLS),
        })
        process_history.append({"label": "final paused public-feed helper", "pid": process.pid, "state": "RUNNING_FOR_MAIN_CUA"})
        run["final_session"] = session_facts(final_session)
        run["final_events"] = final_events
        run["final_candle_history"] = final_history
        run["final_causal_provenance"] = final_provenance
        run["final_virtual_economics"] = final_economics
        run["final_pause_readback"] = final_readback
        run["final_session_states"] = states
        run["tool_names_called"] = sorted(invoked)
        run["final_helper_pid"] = process.pid
        run["final_process_state"] = "PAUSED_SESSION_READY_FOR_MAIN_CUA"
        handoff_ready = True
    except Exception as error:
        failure = f"{type(error).__name__}: {error}"
        run["failure"] = failure
        if process is not None:
            try:
                process_history.append(support.stop_owned_process(process, "failed public-feed helper"))
            except Exception as stop_error:
                process_history.append({"label": "failed public-feed helper", "pid": process.pid, "stop_error": str(stop_error)})
            process = None

    run["status"] = "PASS_FOR_DISPOSABLE_PUBLIC_FEED_AND_PAPER_RECOVERY" if handoff_ready else "FAIL_OR_BLOCKED_NO_LIVE_HANDOFF"
    run["handoff_ready"] = handoff_ready
    run["whole_p13_acceptance_claimed"] = False
    receipt_path = evidence_dir / "paper-public-feed-recovery-receipt.json"
    run["helper_pids"] = process_history
    run["helper_commands"] = commands
    run["checks"] = checks
    run["tool_call_log"] = tool_call_log
    write_receipt(receipt_path, run)

    app_argv = [
        "/usr/bin/env",
        f"SERVICE_P13_REVIEW_ROOT={root}",
        f"SERVICE_P13_REVIEW_PORT={port}",
        f"SERVICE_DESKTOP_ACCESS_TOKEN={token}",
        str(app_executable),
        "--gui-review",
    ] if handoff_ready else None
    print(json.dumps({
        "status": run["status"],
        "receipt": str(receipt_path),
        "root": str(root),
        "data_root": str(data_root),
        "port": port,
        "desktop_endpoint": run["desktop_endpoint"],
        "desktop_token": token if handoff_ready else "not handed off",
        "app_command_argv": app_argv,
        "helper_pid": run.get("final_helper_pid"),
        "session_id": session_id,
        "state": run.get("final_session", {}).get("state"),
        "sequence": run.get("final_session", {}).get("sequence"),
        "processed_candles": run.get("final_session", {}).get("markets", [{}])[0].get("event_count") if run.get("final_session", {}).get("markets") else None,
        "fill_count": run.get("final_session", {}).get("markets", [{}])[0].get("fill_count") if run.get("final_session", {}).get("markets") else None,
        "error": failure,
    }, indent=2, ensure_ascii=False), flush=True)
    return 0 if handoff_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
