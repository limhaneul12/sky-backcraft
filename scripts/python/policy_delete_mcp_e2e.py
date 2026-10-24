#!/usr/bin/env python3
"""Exercise Desktop policy deletion using disposable synthetic policies only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
import hashlib
import http.client
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


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--expect-unavailable", action="store_true")
    args = parser.parse_args()
    binary = args.binary.resolve()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    root = Path(tempfile.mkdtemp(prefix="policy-delete-mcp-"))
    data = root / "research"
    env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR", "LANG") if k in os.environ}
    token = "d" * 64
    env["SERVICE_DESKTOP_ACCESS_TOKEN"] = token
    checks = []
    receipt = {"started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "binary": str(binary), "binary_sha256": sha(binary),
               "script_sha256": sha(Path(__file__)), "isolated_root": str(root),
               "real_exchange_requests": 0, "checks": checks, "status": "RUNNING"}
    process = None
    log = None
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    protocol = "2025-03-26"

    def verify(name, condition):
        checks.append({"name": name, "result": "PASS" if condition else "FAIL"})
        assert condition, name

    def rpc(method, params):
        # Stay below the existing process-wide 20 requests/second ceiling;
        # the two explicit race calls still run concurrently.
        time.sleep(0.1)
        request = urllib.request.Request(f"http://127.0.0.1:{port}/desktop/mcp", data=json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(), headers={
                "Authorization": "Bearer " + token, "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": protocol})
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
        if "error" in payload:
            raise RuntimeError(payload["error"])
        return payload["result"]

    def call(name, arguments, error=False):
        result = rpc("tools/call", {"name": name, "arguments": arguments})
        value = json.loads(result["content"][0]["text"])
        if result.get("isError") and not error:
            raise RuntimeError(value)
        return value

    def start(label):
        nonlocal process, log, protocol
        log = (evidence / (label + ".log")).open("w")
        process = subprocess.Popen([str(binary), "mcp-serve", "--bind", "127.0.0.1", "--port", str(port),
                                    "--data-root", str(data)], cwd=root, env=env, stdout=log, stderr=log)
        for _ in range(200):
            if process.poll() is not None:
                raise RuntimeError("Disposable helper exited during startup")
            try:
                protocol = rpc("initialize", {"protocolVersion": protocol, "capabilities": {},
                    "clientInfo": {"name": "synthetic-policy-delete-e2e", "version": "1"}})["protocolVersion"]
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("Disposable helper readiness timeout")

    def stop():
        nonlocal process, log
        if process and process.poll() is None:
            process.send_signal(signal.SIGINT)
            process.wait(timeout=35)
        if log:
            log.close()
        process = None
        log = None

    def rows():
        with sqlite3.connect(data.joinpath("lab.sqlite").as_uri() + "?mode=ro", uri=True) as db:
            return dict(db.execute("SELECT policy_id, origin FROM policies"))

    def preview(policy_id):
        value = call("resource_delete_preview", {"kind": "policy", "policy_id": policy_id})
        return {"policy_id": policy_id, "preview": {k: value[k] for k in
                ("scope_digest", "expires_at", "confirmation_token")}}

    def request(rid, ids):
        return {"request_id": rid, "items": [preview(pid) for pid in ids],
                "confirmation_text": ids[0] if len(ids) == 1 else f"DELETE {len(ids)} POLICIES"}

    def query(rid):
        return call("policy_delete_query", {"request_id": rid})

    try:
        ids = []
        if not args.expect_unavailable:
            for index in range(6):
                definition = {"schema_version": "1.0", "name": f"Disposable delete fixture {index}",
                    "description": "Synthetic local-only deletion acceptance", "program": {"kind": "RULES", "program": {
                        "indicators": [{"id": "close", "indicator": {"kind": "CLOSE"}}], "states": [],
                        "rules": [{"id": "hold_positive", "condition": {"op": "COMPARE", "comparison": "GT",
                            "left": {"op": "INDICATOR", "id": "close"}, "right": {"op": "CONSTANT", "value": 0.0}},
                            "target": {"kind": "WEIGHT", "value": {"op": "CONSTANT", "value": 0.001}}}],
                        "fallback": {"kind": "HOLD"}, "signal_expiry": {"kind": "END_OF_RANGE"}}}}
                path = root / f"policy-{index}.json"
                path.write_text(json.dumps({"action": "create", "request_id": f"seed-{index}", "definition": definition}))
                result = subprocess.run([str(binary), "policy-write", "--request", str(path), "--data-root", str(data)],
                    cwd=root, env=env, capture_output=True, text=True, timeout=20)
                if result.returncode:
                    raise RuntimeError(result.stderr)
                value = json.loads(result.stdout)
                def find_id(node):
                    if isinstance(node, dict):
                        if "policy_id" in node:
                            return node["policy_id"]
                        return next((found for item in node.values() if (found := find_id(item))), None)
                    return None
                ids.append(find_id(value))
            verify("six independent synthetic USER identities", len(set(ids)) == 6 and None not in ids)
        start("first-process")
        names = {item["name"] for item in rpc("tools/list", {})["tools"]}
        verify("generic hard delete excluded from Desktop", "resource_hard_delete" not in names)
        new_tools = {"policy_delete_write", "policy_delete_query"}
        if args.expect_unavailable:
            verify("historical helper lacks new policy workflow", not new_tools.intersection(names))
            receipt["status"] = "EXPECTED_BASELINE_GAP"
            return
        verify("policy deletion tools available", new_tools.issubset(names))
        capability = call("lab_status", {})["policy_deletion"]
        verify("bounded typed capability", capability == {"mode": "POLICY_ONLY_CONFIRMED", "max_items": 100,
               "archive_available": False, "request_readback": True, "confirmation_required": True})
        before = rows()
        builtin = next(key for key, origin in before.items() if origin == "BUILTIN")
        first = request("delete-single", [ids[0]])
        complete = call("policy_delete_write", first)
        verify("single USER policy deleted with durable scope echo", complete["state"] == "COMPLETE" and
               complete["items"][0]["preview_scope_digest"] == first["items"][0]["preview"]["scope_digest"] and ids[0] not in rows())
        verify("same request returns identical receipt", call("policy_delete_write", first) == complete and query("delete-single") == complete)
        altered = copy.deepcopy(first)
        altered["confirmation_text"] = "different"
        conflict = call("policy_delete_write", altered, error=True)
        verify("different body conflicts without effects", conflict["error"]["code"] == "REQUEST_ID_CONFLICT" and rows() == {k: v for k, v in before.items() if k != ids[0]})
        oversized = copy.deepcopy(first)
        oversized["items"] *= 101
        rejected_size = call("policy_delete_write", oversized, error=True)
        receipt["oversized_reused_response"] = rejected_size
        verify("oversized reused ID rejects before lookup without changing receipt",
               rejected_size["error"]["code"] == "SELECTION_LIMIT" and query("delete-single") == complete)
        mixed = call("policy_delete_write", request("delete-mixed", [builtin, ids[1]]))
        verify("mixed batch protects builtin and deletes USER", mixed["state"] == "PARTIAL" and
               mixed["counts"]["blocked"] == 1 and mixed["counts"]["deleted"] == 1 and builtin in rows() and ids[1] not in rows())
        tampered = request("delete-tampered", [ids[2]])
        tampered["items"][0]["preview"]["confirmation_token"] = "0" * 64
        rejected = call("policy_delete_write", tampered)
        verify("altered preview never deletes", rejected["counts"]["deleted"] == 0 and ids[2] in rows())
        race_a = request("delete-race-a", [ids[3]])
        race_b = copy.deepcopy(race_a)
        race_b["request_id"] = "delete-race-b"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda req: call("policy_delete_write", req), [race_a, race_b]))
        verify("two request IDs cannot delete twice", sum(item["counts"]["deleted"] for item in results) == 1)
        lost = request("delete-lost-response", [ids[4]])
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        connection.request("POST", "/desktop/mcp", body=json.dumps({"jsonrpc": "2.0", "id": 9,
            "method": "tools/call", "params": {"name": "policy_delete_write", "arguments": lost}}),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json",
                     "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": protocol})
        response = connection.getresponse()  # Receive transport headers only.
        response.close()
        connection.close()  # Discard the RPC result body; never resend the write.
        receipt["response_fault"] = "RPC body discarded after transport headers; write never resent"
        for _ in range(100):
            recovered = query("delete-lost-response")
            if recovered.get("found") and recovered.get("state") != "PENDING":
                break
            time.sleep(0.05)
        verify("lost response resolved using only typed readback", recovered.get("state") == "COMPLETE" and ids[4] not in rows())
        stop()
        start("restarted-process")
        verify("receipt survives process restart unchanged", query("delete-lost-response") == recovered)
        remaining = rows()
        verify("restart preserves protected and unselected policies", remaining == {k: v for k, v in before.items() if k not in {ids[0], ids[1], ids[3], ids[4]}})
        verify("unknown request is explicit not found", query("never-admitted") == {"request_id": "never-admitted", "found": False})
        receipt["responses"] = {"complete": complete, "mixed": mixed, "tampered": rejected,
                                  "race": results, "lost_response_readback": recovered}
        receipt["status"] = "PASS_SCOPED_SYNTHETIC_MCP"
    except Exception as error:
        receipt["status"] = "FAIL"
        receipt["error"] = str(error)
        raise
    finally:
        try:
            stop()
            receipt["owned_helper_closed"] = True
        finally:
            receipt["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
