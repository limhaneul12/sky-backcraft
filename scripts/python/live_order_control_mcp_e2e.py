#!/usr/bin/env python3
"""Isolated current-binary runtime send-switch HTTP test; no exchange credentials.

Only explicit MOCK private adapters are used. No strategy session or order is
started. Successful switch enablement is permission-state proof, not fill proof.
"""
import argparse
import hashlib
import http.client
import http.server
import threading
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    environment = {k: os.environ[k] for k in ["HOME", "PATH", "LANG", "TMPDIR"] if k in os.environ}
    environment["SERVICE_DESKTOP_ACCESS_TOKEN"] = "a" * 64
    checks = []
    wire = {}
    process = None
    log = None
    counter = 0
    last_rpc_at = 0.0
    with tempfile.TemporaryDirectory(prefix="backcraft-live-switch-", dir="/tmp") as directory:
        root = Path(directory)
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        def rpc(tool, arguments, desktop=True, override_url=None):
            nonlocal counter, last_rpc_at
            time.sleep(max(0.0, 0.15 - (time.monotonic() - last_rpc_at)))
            last_rpc_at = time.monotonic()
            counter += 1
            headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"}
            if desktop:
                headers["Authorization"] = "Bearer " + "a" * 64
            path = "desktop/mcp" if desktop else "mcp"
            request = urllib.request.Request(
                override_url or f"http://127.0.0.1:{port}/{path}",
                data=json.dumps({"jsonrpc": "2.0", "id": counter, "method": "tools/call", "params": {"name": tool, "arguments": arguments}}).encode(),
                headers=headers,
            )
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    reply = json.load(response)
            except urllib.error.HTTPError as error:
                return {"isError": True, "http_status": error.code, "retry_after": error.headers.get("Retry-After"), "detail": error.read(2048).decode(errors="replace")}
            if "error" in reply:
                return {"isError": True, "rpc_error": reply["error"]}
            return reply["result"]

        def call(arguments, allow_error=False):
            result = rpc("live_order_control", arguments)
            if result.get("isError"):
                if allow_error:
                    return result
                raise AssertionError(result)
            return json.loads(result["content"][0]["text"])

        def status():
            value = call({"action": "status"})
            assert value["exchange_read_mode"] == "MOCK", value
            return value

        def mutate(action, request_id, current=None, confirmation=True, **overrides):
            current = current or status()
            command = {"action": action, "request_id": request_id, "expected_epoch": current["boot_epoch"], "expected_sequence": current["sequence"]}
            if action == "enable" and confirmation:
                command["confirmation"] = "ENABLE_LIVE_ORDER_SENDING"
            command.update(overrides)
            return command, call(command, allow_error=True)

        def start(label, available=True):
            nonlocal process, log
            log = (evidence / f"{label}.log").open("w")
            command = [str(binary), "mcp-serve", "--bind", "127.0.0.1", "--port", str(port), "--data-root", str(root / "research")]
            if available:
                command.append("--live-order-control")
            process = subprocess.Popen(command, cwd=root, env=environment, stdout=log, stderr=log)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise AssertionError(f"isolated MOCK server exited ({label}); see log")
                try:
                    current = status()
                    assert current["live_order_control_available"] is available
                    assert current["live_trading_enabled"] is False
                    return current
                except (OSError, urllib.error.URLError):
                    time.sleep(0.1)
            raise AssertionError("isolated MOCK readiness timeout")

        def stop():
            nonlocal process, log
            if process and process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    raise AssertionError("isolated helper did not drain")
            if log:
                log.close()
            process = None
            log = None

        try:
            initial = start("managed-first")
            wire["status_off"] = initial
            checks.append("managed startup OFF, MOCK provenance and capability available")
            _, missing = mutate("enable", "missing-confirmation", confirmation=False)
            assert missing.get("isError") or missing.get("receipt", {}).get("outcome") == "DENIED"
            assert status()["live_trading_enabled"] is False
            checks.append("missing explicit enable confirmation rejected")
            unknown = call({"action":"request", "request_id":"unknown-request"}, allow_error=True)
            assert unknown.get("isError"), unknown
            checks.append("unknown request readback returns explicit error")

            public = rpc("live_order_control", {"action": "enable", "request_id": "public-enable", "expected_epoch": initial["boot_epoch"], "expected_sequence": status()["sequence"], "confirmation": "ENABLE_LIVE_ORDER_SENDING"}, desktop=False)
            assert public.get("isError") and status()["live_trading_enabled"] is False
            checks.append("public unauthenticated enable rejected")

            original_enable, enabled = mutate("enable", "first-enable")
            assert not enabled.get("isError") and enabled["receipt"]["outcome"] == "ACKNOWLEDGED", enabled
            assert status()["live_trading_enabled"] is True
            wire["enabled"] = enabled
            receipt = call({"action": "request", "request_id": "first-enable"})
            assert receipt["receipt"]["request_id"] == "first-enable"
            checks.append("explicit MOCK enable and durable exact request readback")

            on = status()
            _, stale = mutate("disable", "stale-disable", current=on, expected_sequence=on["sequence"] + 1)
            assert stale.get("isError") or stale.get("receipt", {}).get("outcome") == "DENIED"
            assert status()["live_trading_enabled"] is False
            checks.append("even stale OFF request preempts ON without acknowledging wrong CAS")

            _, on_again = mutate("enable", "fresh-enable-after-off")
            assert not on_again.get("isError") and status()["live_trading_enabled"] is True
            original_disable, disabled = mutate("disable", "normal-disable")
            assert not disabled.get("isError") and disabled["receipt"]["outcome"] == "ACKNOWLEDGED", disabled
            assert status()["live_trading_enabled"] is False
            wire["disabled"] = disabled
            replay = call(original_enable, allow_error=True)
            assert status()["live_trading_enabled"] is False, replay
            checks.append("successful OFF and replayed old ON receipt cannot reopen gate")
            _, before_replayed_off = mutate("enable", "on-before-old-off-replay")
            assert not before_replayed_off.get("isError") and status()["live_trading_enabled"] is True
            replayed_off = call(original_disable)
            assert replayed_off["live_trading_enabled"] is False
            assert replayed_off["sequence"] > before_replayed_off["sequence"]
            assert replayed_off["receipt"] == disabled["receipt"], "original OFF receipt must stay immutable"
            checks.append("old OFF replay fails safe with fresh durable sequence and original receipt preserved")

            class DropCommittedReply(http.server.BaseHTTPRequestHandler):
                def do_POST(self):
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    forwarded = urllib.request.Request(
                        f"http://127.0.0.1:{port}/desktop/mcp", data=body,
                        headers={"Authorization":"Bearer " + "a" * 64, "Content-Type":"application/json", "Accept":"application/json, text/event-stream", "MCP-Protocol-Version":"2025-03-26"},
                    )
                    with urllib.request.urlopen(forwarded, timeout=20) as response:
                        self.server.committed_response = response.read()
                    # The real backend has finished its durable ACK; deliberately
                    # sever this test transport without giving that reply to its caller.
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True

                def log_message(self, *unused):
                    pass

            proxy = http.server.HTTPServer(("127.0.0.1", 0), DropCommittedReply)
            proxy_thread = threading.Thread(target=proxy.handle_request, daemon=True)
            proxy_thread.start()
            before_lost_reply = status()
            lost_command = {"action":"enable", "request_id":"enable-lost-ack", "expected_sequence":before_lost_reply["sequence"], "expected_epoch":before_lost_reply["boot_epoch"], "confirmation":"ENABLE_LIVE_ORDER_SENDING"}
            lost = False
            try:
                rpc("live_order_control", lost_command, override_url=f"http://127.0.0.1:{proxy.server_port}/drop-reply")
            except (http.client.RemoteDisconnected, ConnectionResetError):
                lost = True
            finally:
                proxy_thread.join(timeout=25)
                proxy.server_close()
            assert lost and not proxy_thread.is_alive() and proxy.committed_response
            recovered = call({"action":"request", "request_id":"enable-lost-ack"})
            assert recovered["receipt"]["outcome"] == "ACKNOWLEDGED" and recovered["live_trading_enabled"] is True
            _, after_lost_reply = mutate("disable", "disable-after-lost-ack")
            assert not after_lost_reply.get("isError") and status()["live_trading_enabled"] is False
            checks.append("real dropped transport reply after durable ACK recovered by original request ID without resend")

            last_enable, last = mutate("enable", "enable-before-restart")
            assert not last.get("isError") and status()["live_trading_enabled"] is True
            old_epoch = last_enable["expected_epoch"]
            stop()
            restarted = start("managed-restarted")
            wire["restarted_off"] = restarted
            assert restarted["boot_epoch"] != old_epoch
            call(last_enable, allow_error=True)
            assert status()["live_trading_enabled"] is False
            old_receipt = call({"action": "request", "request_id": "enable-before-restart"})
            assert old_receipt["receipt"]["request_id"] == "enable-before-restart"
            assert old_receipt["live_trading_enabled"] is False
            wire["prior_boot_receipt"] = old_receipt
            _, wrong_epoch = mutate("enable", "stale-boot-new-id", expected_epoch=old_epoch)
            assert wrong_epoch.get("isError") or wrong_epoch.get("receipt", {}).get("outcome") == "DENIED"
            assert status()["live_trading_enabled"] is False
            checks.append("restart resets OFF, advances boot epoch and rejects old-epoch ON")
            stop()

            hard_off = start("unmanaged-hard-off", available=False)
            _, denied = mutate("enable", "capability-absent", current=hard_off)
            assert denied.get("isError") or denied.get("receipt", {}).get("outcome") == "DENIED"
            assert status()["live_trading_enabled"] is False
            checks.append("unmanaged standalone server remains hard OFF")
            stop()
            connection = sqlite3.connect(f"file:{root / 'research/lab.sqlite'}?mode=ro", uri=True)
            counts = {table: connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0] for table in ["automation_sessions", "automation_orders", "automation_fills"]}
            fail_safe_closures = connection.execute("SELECT COUNT(*) FROM live_order_control_fail_safe_closures").fetchone()[0]
            connection.close()
            assert fail_safe_closures >= 1, "replayed OFF lacks durable audit"
            assert all(value == 0 for value in counts.values()), counts
            checks.append("zero strategy sessions, orders and fills; isolated SQLite only")
            receipt = {"status": "PASS_SCOPED_ISOLATED_MOCK_SEND_SWITCH_HTTP", "checks": checks, "counts": counts, "fail_safe_closure_count": fail_safe_closures, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "verification_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "credentials_inherited": False, "real_order_test_cancel_calls": False, "production_db_used": False, "final_switch": "OFF", "scope": "Switch state/permission and restart proof with real Rust HTTP/storage and MOCK adapter; no real venue or strategy execution"}
            wire_text = json.dumps(wire, indent=2) + "\n"
            receipt["wire_sha256"] = hashlib.sha256(wire_text.encode()).hexdigest()
            (evidence / "wire.json").write_text(wire_text)
            (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
            print(json.dumps(receipt, indent=2))
        finally:
            stop()


if __name__ == "__main__":
    main()
