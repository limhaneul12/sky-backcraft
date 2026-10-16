#!/usr/bin/env python3
"""Exercise persistent SIMULATED paper lifecycle/recovery on the pinned release helper.

This fixture always creates a fresh /private/tmp app-support root and gives the Rust
helper only that root's `research` directory. It never opens an existing application
database, loads exchange credentials, calls a private exchange tool, or fabricates a
candle feed. Main explicitly authorized anonymous public finalized-candle GETs.

On success the final helper remains alive with a second session PAUSED so a separate
AppKit review process can attach using SERVICE_P13_REVIEW_ROOT/PORT and the fake desktop
token printed by this script. Stop only the exact PIDs in the generated receipt after
that review is finished.
"""

from __future__ import annotations

import argparse
import datetime as dt
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
import uuid
from typing import Any
import urllib.error
import urllib.request


EXPECTED_RELEASE_SHA256 = "5b6f99aad57f296c68d9fe4d8c658b56276ecf4f6f5db5ab5fe5b2c428744f20"
DEFAULT_BINARY = Path("/Volumes/256.SSD/RustBuild/sky-backcraft/release/spot-lab")
DEFAULT_APP_EXECUTABLE = Path(
    "/tmp/backcraft-p13-packaged-review-mxsl_y7h/P13 Packaged Review.app/Contents/MacOS/SkyBackcraft"
)
PROTOCOL_VERSION = "2025-03-26"


class HarnessError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def current_source_digest(workspace: Path) -> str:
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


def require_real_directory(path: Path) -> None:
    path.lstat()
    if path.is_symlink() or not path.is_dir():
        raise HarnessError(f"expected a regular directory, got {path}")


def allocate_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def sanitized_environment(desktop_token: str, tmpdir: Path) -> dict[str, str]:
    inherited = os.environ
    env: dict[str, str] = {
        "PATH": inherited.get("PATH", "/usr/bin:/bin"),
        "HOME": inherited["HOME"],
        "TMPDIR": str(tmpdir),
        "LANG": inherited.get("LANG", "C.UTF-8"),
        "SERVICE_DESKTOP_ACCESS_TOKEN": desktop_token,
    }
    # A deliberately minimal locale setup keeps helper output deterministic.
    if "LC_CTYPE" in inherited:
        env["LC_CTYPE"] = inherited["LC_CTYPE"]
    return env


class McpClient:
    def __init__(self, port: int, desktop_token: str, tool_call_log: list[dict[str, Any]] | None = None) -> None:
        self.url = f"http://127.0.0.1:{port}/desktop/mcp"
        self.token = desktop_token
        self.protocol_version: str | None = None
        self.next_id = 1
        self.last_request_at = 0.0
        self.tool_call_log = tool_call_log if tool_call_log is not None else []

    def rpc(self, method: str, params: dict[str, Any] | None = None, timeout: float = 30) -> dict[str, Any]:
        remaining = 0.06 - (time.monotonic() - self.last_request_at)
        if self.last_request_at and remaining > 0:
            time.sleep(remaining)
        request_id = self.next_id
        self.next_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.protocol_version is not None:
            headers["MCP-Protocol-Version"] = self.protocol_version
        request = urllib.request.Request(self.url, json.dumps(payload).encode(), headers)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.load(response)
        self.last_request_at = time.monotonic()
        if body.get("id") != request_id:
            raise HarnessError(f"{method}: JSON-RPC response ID mismatch")
        if "error" in body:
            raise HarnessError(f"{method}: {body['error']}")
        result = body.get("result")
        if not isinstance(result, dict):
            raise HarnessError(f"{method}: missing result object")
        return result

    def initialize(self) -> dict[str, Any]:
        result = self.rpc(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "paper-session-recovery-harness", "version": "1"},
            },
        )
        protocol = result.get("protocolVersion")
        if not isinstance(protocol, str) or not protocol:
            raise HarnessError("initialize: server omitted negotiated protocolVersion")
        self.protocol_version = protocol
        return result

    @staticmethod
    def result_text(result: dict[str, Any]) -> str | None:
        for item in result.get("content", []):
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                return item["text"]
        return None

    def call(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        allow_error: bool = False,
        timeout: float = 30,
    ) -> dict[str, Any]:
        self.tool_call_log.append(
            {
                "name": tool_name,
                "action": arguments.get("action"),
                "request_id": arguments.get("request_id"),
                "argument_keys": sorted(arguments),
            }
        )
        result = self.rpc("tools/call", {"name": tool_name, "arguments": arguments}, timeout=timeout)
        if result.get("isError") is True:
            text = self.result_text(result) or "tool returned an unspecified error"
            if allow_error:
                return {"is_error": True, "text": text}
            raise HarnessError(f"{tool_name}: {text}")
        text = self.result_text(result)
        if text is None:
            raise HarnessError(f"{tool_name}: response has no text payload")
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise HarnessError(f"{tool_name}: response text is not JSON: {text[:200]}") from error
        if not isinstance(value, dict):
            raise HarnessError(f"{tool_name}: expected a JSON object response")
        return value


def wait_for_ready(
    binary_process: subprocess.Popen[bytes],
    port: int,
    token: str,
    tool_call_log: list[dict[str, Any]],
) -> McpClient:
    deadline = time.monotonic() + 25
    last_error = "server not ready"
    while time.monotonic() < deadline:
        if binary_process.poll() is not None:
            raise HarnessError(
                f"helper PID {binary_process.pid} exited during startup with {binary_process.returncode}"
            )
        client = McpClient(port, token, tool_call_log)
        try:
            client.initialize()
            return client
        except (urllib.error.URLError, TimeoutError, HarnessError, OSError) as error:
            last_error = str(error)
            time.sleep(0.1)
    raise HarnessError(f"helper did not become ready on 127.0.0.1:{port}: {last_error}")


def stop_owned_process(process: subprocess.Popen[bytes], label: str, timeout: float = 40) -> dict[str, Any]:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Only the exact child PID created by this harness is eligible for fallback.
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        raise HarnessError(f"owned {label} PID {process.pid} did not join after SIGINT")
    result = {"label": label, "pid": process.pid, "returncode": process.returncode, "signal": "SIGINT"}
    if process.returncode != 0:
        raise HarnessError(f"owned {label} PID {process.pid} exited {process.returncode} after SIGINT")
    return result


def require_check(checks: list[dict[str, Any]], name: str, passed: bool, details: Any = None) -> None:
    checks.append({"name": name, "result": "PASS" if passed else "FAIL", "details": details})
    if not passed:
        raise HarnessError(f"check failed: {name}: {details}")


def write_with_readback(
    client: McpClient,
    request_id: str,
    arguments: dict[str, Any],
    *,
    timeout: float = 30,
) -> dict[str, Any]:
    """Reconcile an uncertain mutation by its durable request ID; never resend."""
    try:
        response = client.call("paper_session_write", arguments, timeout=timeout)
        response["_resolution"] = "DIRECT_RESPONSE"
        return response
    except Exception as error:
        readback = client.call("paper_session_query", {"action": "request", "request_id": request_id})
        if readback.get("found") is True and isinstance(readback.get("receipt"), dict) and isinstance(readback.get("session"), dict):
            return {
                "receipt": readback["receipt"],
                "session": readback["session"],
                "_resolution": "DURABLE_REQUEST_READBACK",
            }
        raise HarnessError(
            f"paper request {request_id} reply was not confirmed; readback found={readback.get('found')}; no retry was sent: {error}"
        ) from error


def query_session(client: McpClient, action: str, session_id: str) -> dict[str, Any]:
    result = client.call("paper_session_query", {"action": action, "session_id": session_id})
    session = result.get("session")
    if not isinstance(session, dict) or session.get("session_id") != session_id:
        raise HarnessError(f"paper {action}: session identity mismatch")
    return session


def read_events(
    client: McpClient,
    session_id: str,
    *,
    after_sequence: int = 0,
    limit: int = 50,
) -> list[dict[str, Any]]:
    cursor = after_sequence
    collected: list[dict[str, Any]] = []
    while True:
        page = client.call(
            "paper_session_query",
            {
                "action": "events",
                "session_id": session_id,
                "after_sequence": cursor,
                "limit": limit,
            },
        )
        if page.get("session_id") != session_id:
            raise HarnessError("paper events: session_id changed across cursor pages")
        items = page.get("items")
        if not isinstance(items, list):
            raise HarnessError("paper events: items is not an array")
        for event in items:
            sequence = event.get("sequence")
            if not isinstance(sequence, int) or sequence <= cursor:
                raise HarnessError(f"paper events: non-monotonic sequence {sequence} after {cursor}")
            if collected and sequence <= collected[-1]["sequence"]:
                raise HarnessError("paper events: duplicate or regressed sequence across pages")
            collected.append(event)
            cursor = sequence
        next_cursor = page.get("next_cursor")
        if next_cursor is None:
            break
        next_sequence = next_cursor.get("sequence")
        if not isinstance(next_sequence, int) or next_sequence != cursor:
            raise HarnessError("paper events: next_cursor does not equal last returned sequence")
        if not items:
            raise HarnessError("paper events: cursor made no progress")
    return collected


def make_start_request(options: dict[str, Any], request_id: str) -> dict[str, Any]:
    supported = [item for item in options.get("policy_options", []) if item.get("supported") is True]
    if not supported:
        raise HarnessError("paper start_options has no supported policy option")
    # Prefer the repository's simple buy-and-hold policy when present, otherwise
    # take the first supported option returned by the authority.
    policy = next(
        (item for item in supported if item.get("reference", {}).get("policy_id") == "builtin-buy-and-hold"),
        supported[0],
    )
    presets = options.get("scenario_presets", [])
    decisions = options.get("decision_intervals", [])
    resolutions = options.get("execution_resolutions", [])
    if not presets or not decisions or not resolutions:
        raise HarnessError("paper start_options omitted scenario/interval choices")
    preset = presets[0]
    decision = "H1" if "H1" in decisions else decisions[0]
    resolution = "H1" if "H1" in resolutions else resolutions[0]
    return {
        "action": "start",
        "request_id": request_id,
        "policy_ref": policy["reference"],
        "decision_interval": decision,
        "execution_resolution": resolution,
        "latency_ms": preset["latency_ms"],
        "execution": preset["execution"],
        "markets": [
            {
                "market": "KRW-BTC",
                "initial_cash": preset["initial_cash"],
                "costs": preset["costs"],
                "market_rules": preset["market_rules"],
            }
        ],
    }


def session_facts(session: dict[str, Any]) -> dict[str, Any]:
    markets = []
    for market in session.get("markets", []):
        markets.append(
            {
                key: market.get(key)
                for key in (
                    "market", "model_id", "freshness", "last_source_event_at",
                    "last_finalized_candle_at", "last_account_mark_at", "last_heartbeat_at",
                    "mark_price", "equity", "fill_count", "event_count", "verification",
                )
            }
        )
    return {
        "session_id": session.get("session_id"),
        "state": session.get("state"),
        "sequence": session.get("sequence"),
        "mode": session.get("mode"),
        "policy_ref": session.get("policy_ref"),
        "decision_interval": session.get("decision_interval"),
        "execution_resolution": session.get("execution_resolution"),
        "markets": markets,
    }


def event_summaries(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = []
    for event in events:
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        candle = event.get("candle") or {}
        summaries.append(
            {
                "sequence": event.get("sequence"),
                "kind": event.get("kind"),
                "market": event.get("market"),
                "source_event_at": event.get("source_event_at"),
                "finalized_at": event.get("finalized_at"),
                "candle_observation_id": candle.get("observation_id"),
                "candle_content_digest": candle.get("content_digest"),
                "signal_count": len(event.get("signals", [])),
                "order_count": len(event.get("orders", [])),
                "fill_count": len(event.get("fills", [])),
                "account_mark_count": len(event.get("account_marks", [])),
                "event_sha256": hashlib.sha256(encoded).hexdigest(),
            }
        )
    return summaries


def make_root() -> Path:
    base = Path("/private/tmp")
    require_real_directory(base)
    root = Path(tempfile.mkdtemp(prefix="sky-paper-recovery-", dir=base))
    require_real_directory(root)
    return root


def write_review_config(root: Path, port: int) -> Path:
    config = root / "settings.json"
    payload = {
        "domain": "",
        "port": port,
        "authenticationMode": "none",
    }
    descriptor = os.open(config, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        json.dump(payload, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.write("\n")
    return config


def build_receipt(
    *,
    workspace: Path,
    binary: Path,
    binary_digest: str,
    script_digest: str,
    root: Path,
    data_root: Path,
    settings_path: Path,
    port: int,
    desktop_token: str,
    helper_pids: list[dict[str, Any]],
    final_pid: int | None,
    checks: list[dict[str, Any]],
    source_digest: str | None,
    source_matches: bool | None,
    request_ids: dict[str, str],
    session_records: dict[str, Any],
    tool_call_log: list[dict[str, Any]],
    child_environment_keys: list[str],
    helper_commands: list[list[str]],
    failure: str | None,
) -> dict[str, Any]:
    return {
        "time_utc": utc_now(),
        "status": "PASS_FOR_DISPOSABLE_RELEASE_HELPER_MCP" if failure is None else "FAIL",
        "scope": "Loopback private-desktop MCP paper lifecycle and helper process restart using one fresh SIMULATED root.",
        "input_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "current_rust_source_digest": current_source_digest(workspace),
        "helper_reported_source_digest": source_digest,
        "helper_reported_source_matches_current_workspace": source_matches,
        "scope_audit_sha256": sha256_file(workspace / ".omx/evidence/p13/goal-paper-recovery-scope-audit.md"),
        "current_source_input_sha256": {
            relative: sha256_file(workspace / relative)
            for relative in (
                "crates/lab/src/contracts/paper.rs",
                "crates/lab/src/storage/paper_store.rs",
                "crates/lab/src/jobs/research_control.rs",
                "crates/lab/src/market_data.rs",
                "crates/lab/src/mcp.rs",
                "crates/lab/src/main.rs",
            )
        },
        "helper_binary": str(binary),
        "helper_binary_sha256": binary_digest,
        "verification_script_sha256": script_digest,
        "temporary_root": str(root),
        "data_root": str(data_root),
        "sqlite_path": str(data_root / "lab.sqlite"),
        "settings_path": str(settings_path),
        "app_configuration": {"domain": "", "port": port, "authenticationMode": "none"},
        "desktop_endpoint": f"http://127.0.0.1:{port}/desktop/mcp",
        "desktop_token_sha256": hashlib.sha256(desktop_token.encode()).hexdigest(),
        "desktop_token_value_in_evidence": False,
        "helper_commands": helper_commands,
        "helper_processes": helper_pids,
        "final_paused_helper_pid": final_pid,
        "request_ids": request_ids,
        "sessions": session_records,
        "tool_call_log": tool_call_log,
        "tool_names_called": sorted({item["name"] for item in tool_call_log}),
        "helper_environment_keys": child_environment_keys,
        "checks": checks,
        "network_scope": {
            "anonymous_public_candle_gets_authorized_by_main": True,
            "private_exchange_credentials_inherited": False,
            "private_exchange_tools_called": False,
            "real_exchange_order_cancel_transfer_withdrawal_calls": False,
            "actual_public_candle_requests_may_occur_during_start_resume": True,
            "network_sandbox_used": False,
        },
        "candle_feed_boundary": {
            "supported_deterministic_feed_injection_used": False,
            "paper_market_start_input_fields": ["market", "initial_cash", "costs", "market_rules"],
            "supported_feed_override_field_present": False,
            "fabricated_cli_candles": False,
            "duplicate_or_missing_candle_fault_injected": False,
            "bar_processing_claimed": False,
            "note": "The release CLI exposes no supported deterministic paper-candle injection input. Public GET is allowed; a real returned bar may be processed and is recorded from the durable paper events only.",
        },
        "failure": failure,
        "whole_p13_acceptance_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--evidence", type=Path, default=Path(".omx/evidence/p13/paper-recovery"))
    parser.add_argument("--app-executable", type=Path, default=DEFAULT_APP_EXECUTABLE)
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parents[1]
    binary = args.binary.resolve(strict=True)
    if binary.is_symlink() or not os.access(binary, os.X_OK):
        parser.error("--binary must be a regular executable release helper")
    binary_digest = sha256_file(binary)
    if binary_digest != EXPECTED_RELEASE_SHA256:
        parser.error(
            f"release helper SHA-256 mismatch: expected {EXPECTED_RELEASE_SHA256}, got {binary_digest}"
        )
    if sys.platform != "darwin":
        parser.error("this P13 packaged AppKit review harness is macOS-only")
    if not os.environ.get("HOME"):
        parser.error("HOME must be preserved from the existing user environment")
    app_executable = args.app_executable.resolve(strict=True)
    if not app_executable.is_file() or not os.access(app_executable, os.X_OK):
        parser.error("--app-executable must name the existing AppKit review executable")

    evidence_dir = args.evidence if args.evidence.is_absolute() else workspace / args.evidence
    evidence_dir.mkdir(parents=True, exist_ok=True)
    if evidence_dir.is_symlink():
        parser.error("evidence directory must not be a symlink")
    run_id = uuid.uuid4().hex
    evidence_dir = evidence_dir / f"paper-recovery-{run_id[:12]}"
    evidence_dir.mkdir(mode=0o755, exist_ok=False)
    root = make_root()
    data_root = root / "research"
    port = allocate_loopback_port()
    if port == 8130:
        raise HarnessError("ephemeral port unexpectedly selected 8130")
    token = uuid.uuid4().hex * 2
    settings_path = write_review_config(root, port)
    app_hash = sha256_file(app_executable)

    request_ids = {
        name: f"p13-paper-{run_id}-{name}" for name in
        ("start", "pause", "stale-cas", "resume", "pause-again", "stop", "handoff-start", "handoff-pause")
    }
    checks: list[dict[str, Any]] = []
    helper_processes: list[dict[str, Any]] = []
    helper_commands: list[list[str]] = []
    tool_call_log: list[dict[str, Any]] = []
    sessions: dict[str, Any] = {}
    session_records: dict[str, Any] = {}
    current_process: subprocess.Popen[bytes] | None = None
    current_client: McpClient | None = None
    final_pid: int | None = None
    helper_reported_digest: str | None = None
    source_matches: bool | None = None
    successful_handoff = False
    failure: str | None = None
    script_digest = sha256_file(Path(__file__).resolve())

    try:
        # Helper listens on loopback; only the anonymous public candle client
        # may leave the process. Private exchange mode remains MOCK because the
        # sanitized environment contains no Upbit credential variables/files.
        env = sanitized_environment(token, root)
        credential_keys = sorted(
            key for key in env
            if key in {
                "SERVICE_UPBIT_PRIVATE_ACCESS_KEY",
                "SERVICE_UPBIT_PRIVATE_SECRET_KEY",
                "SERVICE_UPBIT_CREDENTIAL_FILE",
            }
        )
        require_check(
            checks,
            "helper environment excludes Upbit private credentials/config path",
            not credential_keys,
            {"credential_keys_present": credential_keys, "environment_key_names": sorted(env)},
        )
        require_check(
            checks,
            "fresh helper cwd has no dotenv credential file",
            not (root / ".env").exists() and not (data_root / ".env").exists(),
            {"root_dotenv_present": (root / ".env").exists(), "data_root_dotenv_present": (data_root / ".env").exists()},
        )
        source_digest_expected = current_source_digest(workspace)

        def start_named_process(label: str) -> tuple[subprocess.Popen[bytes], McpClient, list[str]]:
            log_path = evidence_dir / f"{label}.log"
            command = [
                str(binary), "mcp-serve",
                "--bind", "127.0.0.1", "--port", str(port),
                "--data-root", str(data_root), "--public-no-auth",
            ]
            log = log_path.open("ab", buffering=0)
            try:
                process = subprocess.Popen(
                    command,
                    cwd=root,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            finally:
                log.close()
            try:
                client = wait_for_ready(process, port, token, tool_call_log)
            except Exception:
                helper_processes.append(stop_owned_process(process, f"{label} startup failure"))
                raise
            return process, client, command

        current_process, current_client, command = start_named_process("paper-process-1")
        helper_commands.append(command)
        require_check(
            checks,
            "exact release helper binary",
            sha256_file(binary) == EXPECTED_RELEASE_SHA256,
            {"path": str(binary), "sha256": binary_digest},
        )
        require_check(
            checks,
            "loopback desktop MCP with app-compatible fake token",
            current_client.protocol_version == PROTOCOL_VERSION,
            {"endpoint": current_client.url, "protocol": current_client.protocol_version},
        )
        tools = current_client.rpc("tools/list").get("tools", [])
        tool_names = {tool.get("name") for tool in tools}
        require_check(
            checks,
            "desktop paper MCP tool discovery",
            {"paper_session_query", "paper_session_write"}.issubset(tool_names),
            {"paper_session_query": "paper_session_query" in tool_names, "paper_session_write": "paper_session_write" in tool_names},
        )

        status = current_client.call("lab_status", {})
        helper_reported_digest = status.get("source_digest") if isinstance(status.get("source_digest"), str) else None
        source_matches = helper_reported_digest == source_digest_expected
        require_check(
            checks,
            "helper reports its source identity",
            helper_reported_digest is not None,
            {"helper_reported_source_digest": helper_reported_digest, "current_workspace_digest": source_digest_expected, "matches": source_matches},
        )
        private_mode_text = (evidence_dir / "paper-process-1.log").read_text(errors="replace")
        require_check(
            checks,
            "helper selected no-credential MOCK private venue mode",
            "Upbit credentials are absent; using explicit MOCK read mode" in private_mode_text,
            "credential-bearing environment was not inherited",
        )

        options = current_client.call("paper_session_query", {"action": "start_options"})
        request = make_start_request(options, request_ids["start"])
        start_response = write_with_readback(current_client, request_ids["start"], request, timeout=90)
        start_receipt = start_response.get("receipt", {})
        session = start_response.get("session", {})
        session_id = session.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            raise HarnessError("paper start did not return durable session_id")
        require_check(
            checks,
            "supported SIMULATED paper session created",
            start_receipt.get("action") == "start"
            and start_receipt.get("outcome") == "APPLIED"
            and session.get("mode") == "SIMULATED",
            {"session_id": session_id, "sequence": session.get("sequence"), "state": session.get("state"), "mode": session.get("mode")},
        )
        session_records["lifecycle_session"] = {"start": session_facts(session), "request_id": request_ids["start"]}

        # Pause immediately after create: paper reads while RUNNING may make an
        # anonymous public finalized-candle GET, which is the only network lane
        # authorized by this task. The start call itself may already have polled.
        pause_arguments = {
            "action": "pause",
            "request_id": request_ids["pause"],
            "session_id": session_id,
            "expected_sequence": session["sequence"],
        }
        pause_response = write_with_readback(
            current_client,
            request_ids["pause"],
            pause_arguments,
            timeout=90,
        )
        paused = pause_response.get("session", {})
        pause_sequence = paused.get("sequence")
        require_check(
            checks,
            "pause receipt and CAS sequence",
            pause_response.get("receipt", {}).get("outcome") == "APPLIED"
            and paused.get("state") == "PAUSED"
            and isinstance(pause_sequence, int)
            and pause_sequence > int(session.get("sequence", -1)),
            {"session_id": session_id, "sequence": pause_sequence, "state": paused.get("state")},
        )
        session_records["lifecycle_session"]["paused_before_restart"] = session_facts(paused)

        start_replay = current_client.call("paper_session_write", request, timeout=30)
        require_check(
            checks,
            "start request replay is idempotent after pause",
            start_replay.get("receipt", {}).get("outcome") == "REPLAYED"
            and start_replay.get("receipt", {}).get("session_id") == session_id
            and start_replay.get("session", {}).get("sequence") == pause_sequence,
            {"outcome": start_replay.get("receipt", {}).get("outcome"), "session_id": start_replay.get("receipt", {}).get("session_id"), "sequence": start_replay.get("session", {}).get("sequence")},
        )
        start_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["start"]})
        pause_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["pause"]})
        require_check(
            checks,
            "durable start and pause request readbacks",
            start_readback.get("found") is True and pause_readback.get("found") is True
            and start_readback.get("session_id") == session_id and pause_readback.get("session_id") == session_id,
            {"start_found": start_readback.get("found"), "pause_found": pause_readback.get("found"), "session_id": session_id},
        )

        # Persist a sequence fence before shutting down. The next process must
        # reproduce the same durable event tail after this acknowledged cursor.
        acknowledged_cursor = 0
        pre_restart_tail = read_events(current_client, session_id, after_sequence=acknowledged_cursor, limit=50)
        require_check(
            checks,
            "durable pause event is ordered and replayable before restart",
            any(event.get("kind") == "PAUSE" for event in pre_restart_tail)
            and all(a["sequence"] < b["sequence"] for a, b in zip(pre_restart_tail, pre_restart_tail[1:])),
            {"after_sequence": acknowledged_cursor, "sequences": [event["sequence"] for event in pre_restart_tail]},
        )
        session_records["lifecycle_session"]["pre_restart_events_after_0"] = event_summaries(pre_restart_tail)
        session_records["lifecycle_session"]["acknowledged_event_cursor"] = acknowledged_cursor

        first_process = current_process
        current_process = None
        current_client = None
        stopped_first = stop_owned_process(first_process, "first paper helper")
        helper_processes.append(stopped_first)

        current_process, current_client, command = start_named_process("paper-process-2-restarted")
        helper_commands.append(command)
        restarted_session = query_session(current_client, "get", session_id)
        require_check(
            checks,
            "paused session identity and sequence survive helper process restart",
            restarted_session.get("state") == "PAUSED" and restarted_session.get("sequence") == pause_sequence,
            {"before": session_facts(paused), "after": session_facts(restarted_session)},
        )
        post_restart_tail = read_events(current_client, session_id, after_sequence=acknowledged_cursor, limit=50)
        require_check(
            checks,
            "event cursor readback after restart matches the pre-restart durable tail",
            post_restart_tail == pre_restart_tail,
            {"before_sequences": [event["sequence"] for event in pre_restart_tail], "after_sequences": [event["sequence"] for event in post_restart_tail]},
        )
        session_records["lifecycle_session"]["post_restart_events_after_0"] = event_summaries(post_restart_tail)
        empty_tail = current_client.call(
            "paper_session_query",
            {"action": "events", "session_id": session_id, "after_sequence": pause_sequence, "limit": 1},
        )
        require_check(
            checks,
            "event cursor after latest acknowledged sequence is empty",
            empty_tail.get("items") == [] and empty_tail.get("returned_count") == 0,
            {"returned_count": empty_tail.get("returned_count"), "total_count": empty_tail.get("total_count")},
        )
        start_readback_after_restart = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["start"]})
        pause_readback_after_restart = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["pause"]})
        require_check(
            checks,
            "durable request identities survive helper restart",
            start_readback_after_restart.get("found") is True and pause_readback_after_restart.get("found") is True
            and start_readback_after_restart.get("session_id") == session_id and pause_readback_after_restart.get("session_id") == session_id,
            {"start_found": start_readback_after_restart.get("found"), "pause_found": pause_readback_after_restart.get("found")},
        )

        stale_resume = current_client.call(
            "paper_session_write",
            {"action": "resume", "request_id": request_ids["stale-cas"], "session_id": session_id, "expected_sequence": max(0, int(pause_sequence) - 1)},
            allow_error=True,
        )
        unchanged = query_session(current_client, "get", session_id)
        stale_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["stale-cas"]})
        require_check(
            checks,
            "stale expected_sequence is rejected without mutation",
            stale_resume.get("is_error") is True and "CONFLICT" in stale_resume.get("text", "")
            and unchanged.get("state") == "PAUSED" and unchanged.get("sequence") == pause_sequence
            and stale_readback.get("found") is False,
            {"error": stale_resume.get("text"), "session": session_facts(unchanged), "readback_found": stale_readback.get("found")},
        )

        resume_arguments = {
            "action": "resume",
            "request_id": request_ids["resume"],
            "session_id": session_id,
            "expected_sequence": pause_sequence,
        }
        resume_response = write_with_readback(current_client, request_ids["resume"], resume_arguments, timeout=90)
        resumed = resume_response.get("session", {})
        require_check(
            checks,
            "resume after restart uses the exact durable CAS sequence",
            resume_response.get("receipt", {}).get("outcome") == "APPLIED" and resumed.get("state") == "RUNNING"
            and resume_response.get("receipt", {}).get("sequence") == pause_sequence + 1
            and isinstance(resumed.get("sequence"), int)
            and resumed.get("sequence") >= pause_sequence + 1,
            {
                "session_id": session_id,
                "receipt_sequence": resume_response.get("receipt", {}).get("sequence"),
                "refreshed_session_sequence": resumed.get("sequence"),
                "state": resumed.get("state"),
            },
        )

        pause_again_args = {
            "action": "pause",
            "request_id": request_ids["pause-again"],
            "session_id": session_id,
            "expected_sequence": resumed["sequence"],
        }
        pause_again = write_with_readback(current_client, request_ids["pause-again"], pause_again_args, timeout=90)
        paused_again = pause_again.get("session", {})
        stop_args = {
            "action": "stop",
            "request_id": request_ids["stop"],
            "session_id": session_id,
            "expected_sequence": paused_again["sequence"],
        }
        stop_response = write_with_readback(current_client, request_ids["stop"], stop_args, timeout=90)
        stopped = stop_response.get("session", {})
        require_check(
            checks,
            "pause and stop complete with durable CAS receipts",
            pause_again.get("receipt", {}).get("outcome") == "APPLIED"
            and paused_again.get("state") == "PAUSED"
            and stop_response.get("receipt", {}).get("outcome") == "APPLIED"
            and stopped.get("state") == "STOPPED"
            and stopped.get("sequence") == paused_again.get("sequence", 0) + 1,
            {"paused": session_facts(paused_again), "stopped": session_facts(stopped)},
        )
        stop_replay = current_client.call("paper_session_write", stop_args, timeout=30)
        require_check(
            checks,
            "stop request replay does not change stopped sequence",
            stop_replay.get("receipt", {}).get("outcome") == "REPLAYED"
            and stop_replay.get("session", {}).get("state") == "STOPPED"
            and stop_replay.get("session", {}).get("sequence") == stopped.get("sequence"),
            {"outcome": stop_replay.get("receipt", {}).get("outcome"), "sequence": stop_replay.get("session", {}).get("sequence")},
        )
        resume_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["resume"]})
        stop_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["stop"]})
        require_check(
            checks,
            "resume and stop receipts remain readable after helper restart",
            resume_readback.get("found") is True and stop_readback.get("found") is True
            and resume_readback.get("session_id") == session_id and stop_readback.get("session_id") == session_id,
            {"resume_found": resume_readback.get("found"), "stop_found": stop_readback.get("found")},
        )
        final_lifecycle_events = read_events(current_client, session_id, after_sequence=0, limit=50)
        sequences = [event.get("sequence") for event in final_lifecycle_events]
        require_check(
            checks,
            "post-restart event history is monotonic, contiguous, and duplicate-free",
            len(sequences) == len(set(sequences)) and sequences == sorted(sequences)
            and all(right == left + 1 for left, right in zip(sequences, sequences[1:]))
            and (not sequences or sequences[-1] == stopped.get("sequence")),
            {"sequences": sequences, "session_sequence": stopped.get("sequence")},
        )
        session_records["lifecycle_session"].update({
            "resumed": session_facts(resumed),
            "stopped": session_facts(stopped),
            "events": event_summaries(final_lifecycle_events),
        })

        # Main's CUA handle uses a separate session and leaves it PAUSED. This
        # keeps the runtime read projection available without an active feed poll.
        handoff_request = make_start_request(options, request_ids["handoff-start"])
        handoff_start = write_with_readback(current_client, request_ids["handoff-start"], handoff_request, timeout=90)
        handoff_session = handoff_start.get("session", {})
        handoff_id = handoff_session.get("session_id")
        if not isinstance(handoff_id, str) or not handoff_id:
            raise HarnessError("handoff paper start did not return session_id")
        handoff_pause_args = {
            "action": "pause",
            "request_id": request_ids["handoff-pause"],
            "session_id": handoff_id,
            "expected_sequence": handoff_session["sequence"],
        }
        handoff_pause = write_with_readback(current_client, request_ids["handoff-pause"], handoff_pause_args, timeout=90)
        handoff_session = handoff_pause.get("session", {})
        require_check(
            checks,
            "separate AppKit CUA session is durably PAUSED before handoff",
            handoff_pause.get("receipt", {}).get("outcome") == "APPLIED" and handoff_session.get("state") == "PAUSED",
            session_facts(handoff_session),
        )
        handoff_readback = current_client.call("paper_session_query", {"action": "request", "request_id": request_ids["handoff-pause"]})
        require_check(
            checks,
            "CUA handoff pause request has durable readback",
            handoff_readback.get("found") is True and handoff_readback.get("session_id") == handoff_id
            and handoff_readback.get("session", {}).get("state") == "PAUSED",
            {"request_id": request_ids["handoff-pause"], "found": handoff_readback.get("found"), "session_id": handoff_readback.get("session_id")},
        )
        require_check(
            checks,
            "CUA handoff session is distinct from stopped lifecycle session",
            handoff_id != session_id,
            {"lifecycle_session_id": session_id, "cua_session_id": handoff_id},
        )
        handoff_events = read_events(current_client, handoff_id, after_sequence=0, limit=50)
        require_check(
            checks,
            "CUA handoff session exposes its durable pause event",
            any(event.get("kind") == "PAUSE" for event in handoff_events),
            {"sequences": [event["sequence"] for event in handoff_events], "kinds": [event["kind"] for event in handoff_events]},
        )
        listed = current_client.call("paper_session_query", {"action": "list", "limit": 50})
        listed_sessions = {
            item.get("session_id"): item.get("state")
            for item in listed.get("items", [])
            if isinstance(item, dict)
        }
        require_check(
            checks,
            "final disposable database contains no RUNNING paper session",
            listed_sessions.get(session_id) == "STOPPED"
            and listed_sessions.get(handoff_id) == "PAUSED"
            and all(state != "RUNNING" for state in listed_sessions.values()),
            {"session_states": listed_sessions},
        )
        sessions["cua_handoff_session"] = {
            "session": session_facts(handoff_session),
            "start_request_id": request_ids["handoff-start"],
            "pause_request_id": request_ids["handoff-pause"],
            "events": event_summaries(handoff_events),
        }
        sessions["lifecycle_session"] = session_records["lifecycle_session"]
        forbidden_private_tools = {
            "upbit_account_balances", "upbit_orders_open", "upbit_orders_closed",
            "upbit_order_get", "upbit_order_chance", "upbit_order_test",
            "upbit_order_place", "upbit_order_cancel", "trading_control",
        }
        invoked_tools = {item["name"] for item in tool_call_log}
        require_check(
            checks,
            "private account/order/control MCP tools were never invoked",
            not (invoked_tools & forbidden_private_tools),
            {"called_tools": sorted(invoked_tools), "forbidden_called": sorted(invoked_tools & forbidden_private_tools)},
        )
        helper_processes.append({"label": "final paused helper", "pid": current_process.pid, "state": "RUNNING_FOR_MAIN_CUA"})
        final_pid = current_process.pid
        successful_handoff = True
    except Exception as error:
        failure = f"{type(error).__name__}: {error}"
        if current_process is not None:
            try:
                helper_processes.append(stop_owned_process(current_process, "failed test helper"))
            except Exception as stop_error:
                helper_processes.append({"label": "failed test helper", "pid": current_process.pid, "stop_error": str(stop_error)})
            current_process = None

    receipt = build_receipt(
        workspace=workspace,
        binary=binary,
        binary_digest=binary_digest,
        script_digest=script_digest,
        root=root,
        data_root=data_root,
        settings_path=settings_path,
        port=port,
        desktop_token=token,
        helper_pids=helper_processes,
        final_pid=final_pid,
        checks=checks,
        source_digest=helper_reported_digest,
        source_matches=source_matches,
        request_ids=request_ids,
        session_records=sessions,
        tool_call_log=tool_call_log,
        child_environment_keys=sorted(env.keys()) if "env" in locals() else [],
        helper_commands=helper_commands,
        failure=failure,
    )
    receipt["app"] = {
        "executable": str(app_executable),
        "sha256": app_hash,
        "review_root_environment": "SERVICE_P13_REVIEW_ROOT",
        "review_port_environment": "SERVICE_P13_REVIEW_PORT",
        "desktop_token_environment": "SERVICE_DESKTOP_ACCESS_TOKEN",
        "recommended_arguments": ["--gui-review"],
        "do_not_add_argument": "--gui-review-read-only enables live account reads in this app revision",
        "review_settings": {"domain": "", "port": port, "authenticationMode": "none"},
        "recommended_command_template": [
            "/usr/bin/env",
            f"SERVICE_P13_REVIEW_ROOT={root}",
            f"SERVICE_P13_REVIEW_PORT={port}",
            "SERVICE_DESKTOP_ACCESS_TOKEN=<fake-64-hex-token-from-console-handoff>",
            str(app_executable),
            "--gui-review",
        ],
    }
    lifecycle_events = sessions.get("lifecycle_session", {}).get("events", [])
    handoff_event_summaries = sessions.get("cua_handoff_session", {}).get("events", [])
    all_summaries = lifecycle_events + handoff_event_summaries
    candle_events = [event for event in all_summaries if event.get("kind") == "CANDLE_PROCESSED"]
    candle_ids = [event.get("candle_observation_id") for event in candle_events if event.get("candle_observation_id")]
    receipt["candle_feed_boundary"].update(
        {
            "bar_processing_claimed": bool(candle_events),
            "processed_candle_event_count": len(candle_events),
            "unique_candle_observation_ids": len(set(candle_ids)),
            "duplicate_candle_observation_ids_in_observed_events": len(candle_ids) - len(set(candle_ids)),
        }
    )
    receipt["final_process_state"] = "PAUSED_SESSION_READY_FOR_MAIN_CUA" if successful_handoff else "STOPPED_AFTER_FAILURE"
    output_path = evidence_dir / "paper-session-recovery-mcp-receipt.json"
    output_path.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    app_command = [
        "/usr/bin/env",
        f"SERVICE_P13_REVIEW_ROOT={root}",
        f"SERVICE_P13_REVIEW_PORT={port}",
        f"SERVICE_DESKTOP_ACCESS_TOKEN={token}",
        str(app_executable),
        "--gui-review",
    ]
    print(json.dumps({
        "status": receipt["status"],
        "receipt": str(output_path),
        "root": str(root),
        "data_root": str(data_root),
        "port": port,
        "desktop_endpoint": receipt["desktop_endpoint"],
        "desktop_token": token if successful_handoff else "not handed off",
        "app_command_argv": app_command if successful_handoff else None,
        "settings_path": str(settings_path),
        "final_helper_pid": final_pid,
        "handoff_session_id": sessions.get("cua_handoff_session", {}).get("session", {}).get("session_id"),
        "handoff_session_sequence": sessions.get("cua_handoff_session", {}).get("session", {}).get("sequence"),
        "error": failure,
    }, indent=2, ensure_ascii=False))
    return 0 if successful_handoff else 1


if __name__ == "__main__":
    raise SystemExit(main())
