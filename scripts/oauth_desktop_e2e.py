#!/usr/bin/env python3
"""Exercise OAuth and MCP over real loopback HTTP with an isolated backend."""

import argparse
import base64
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from mcp_desktop_smoke import smoke


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ConsentForm(HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "input" and attributes.get("type") == "hidden":
            self.fields[attributes["name"]] = attributes.get("value", "")


def run(binary):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    issuer = "https://skybackcraft.store"
    resource = f"{issuer}/mcp"
    owner = secrets.token_hex(32)
    opener = urllib.request.build_opener(NoRedirect())

    def http(path, payload=None, *, form=False, token=None):
        headers = {"Host": "skybackcraft.store", "Accept": "application/json, text/event-stream",
                   "User-Agent": "SkyBackcraft-Desktop-E2E/1.0"}
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded" if form else "application/json"
            body = urllib.parse.urlencode(payload).encode() if form else json.dumps(payload).encode()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(f"{base}{path}", body, headers)
        try:
            response = opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read().decode()
            return response.status, response.headers, raw

    with tempfile.TemporaryDirectory(prefix="sky-backcraft-oauth-e2e-") as root:
        log_path = Path(root) / "server.log"
        with log_path.open("w+") as log:
            server_arguments = [
                str(binary), "mcp-serve", "--port", str(port),
                "--data-root", str(Path(root) / "research"), "--oauth",
                "--public-url", issuer, "--allow-host", "skybackcraft.store",
            ]
            environment = {**os.environ, "SPOT_LAB_OAUTH_OWNER_CODE": owner}
            child = subprocess.Popen(server_arguments, env=environment, stdout=log, stderr=log)

            def wait_ready():
                deadline = time.monotonic() + 15
                while True:
                    if child.poll() is not None:
                        raise RuntimeError(f"backend exited {child.returncode}")
                    try:
                        status, _, raw = http("/.well-known/oauth-authorization-server")
                        return status, raw
                    except urllib.error.URLError:
                        if time.monotonic() > deadline:
                            raise RuntimeError("backend readiness timed out")
                        time.sleep(0.05)

            try:
                status, raw = wait_ready()
                assert status == 200, f"discovery returned {status}"
                metadata = json.loads(raw)
                assert metadata["issuer"] == issuer
                assert "S256" in metadata["code_challenge_methods_supported"]
                status, headers, _ = http("/mcp", {"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
                assert status == 401 and "resource_metadata" in headers.get("WWW-Authenticate", "")
                status, _, raw = http("/register", {
                    "client_name": "Desktop E2E", "redirect_uris": ["http://127.0.0.1:8765/callback"],
                    "grant_types": ["authorization_code", "refresh_token"], "response_types": ["code"],
                    "token_endpoint_auth_method": "none",
                })
                assert status == 201, f"registration returned {status}"
                client = json.loads(raw)["client_id"]
                verifier = secrets.token_urlsafe(48)
                challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
                parameters = {
                    "response_type": "code", "client_id": client,
                    "redirect_uri": "http://127.0.0.1:8765/callback", "state": "desktop-e2e",
                    "code_challenge": challenge, "code_challenge_method": "S256", "resource": resource,
                }
                status, _, raw = http("/authorize?" + urllib.parse.urlencode(parameters))
                assert status == 200, f"authorization returned {status}"
                parsed = ConsentForm()
                parsed.feed(raw)
                assert parsed.fields, "consent form has no request binding"
                denied, _, _ = http("/authorize/approve", {**parsed.fields, "owner_code": "wrong-owner-code"}, form=True)
                assert denied in (400, 401, 403), f"invalid owner was accepted: {denied}"
                status, _, raw = http("/authorize?" + urllib.parse.urlencode(parameters))
                assert status == 200
                parsed = ConsentForm()
                parsed.feed(raw)
                status, headers, _ = http("/authorize/approve", {**parsed.fields, "owner_code": owner}, form=True)
                assert status in (302, 303), f"consent returned {status}"
                callback = urllib.parse.urlsplit(headers["Location"])
                query = urllib.parse.parse_qs(callback.query)
                assert query["state"] == ["desktop-e2e"]
                assert callback.netloc == "127.0.0.1:8765"
                token_request = {"grant_type": "authorization_code", "client_id": client,
                                 "redirect_uri": parameters["redirect_uri"], "code": query["code"][0],
                                 "code_verifier": verifier, "resource": resource}
                status, _, raw = http("/token", token_request, form=True)
                assert status == 200, f"token exchange returned {status}"
                tokens = json.loads(raw)
                token = tokens["access_token"]
                replay, _, _ = http("/token", token_request, form=True)
                assert replay == 400, f"authorization code replay returned {replay}"
                result = smoke(f"{base}/mcp", token)
                assert result["transport"]["auth"] == "oauth"
                refresh_request = {"grant_type": "refresh_token", "client_id": client,
                                   "refresh_token": tokens["refresh_token"], "resource": resource}
                refreshed, _, raw = http("/token", refresh_request, form=True)
                assert refreshed == 200, f"refresh returned {refreshed}"
                assert json.loads(raw)["refresh_token"] != tokens["refresh_token"]
                refresh_replay, _, _ = http("/token", refresh_request, form=True)
                assert refresh_replay == 400, f"refresh token replay returned {refresh_replay}"
                result.update({"discovery": "PASS", "owner_consent": "PASS", "pkce_exchange": "PASS",
                               "unauthorized_rejection": "PASS", "code_replay_rejection": "PASS",
                               "refresh_rotation": "PASS", "refresh_replay_rejection": "PASS"})
                child.send_signal(signal.SIGINT)
                child.wait(timeout=40)
                assert child.returncode == 0, f"first backend exit {child.returncode}"
                child = subprocess.Popen(server_arguments, env=environment, stdout=log, stderr=log)
                status, _ = wait_ready()
                assert status == 200
                denied, _, _ = http("/mcp", {"jsonrpc": "2.0", "id": 3, "method": "tools/list"}, token=token)
                assert denied == 401, f"old bearer survived restart: HTTP {denied}"
                result["restart_invalidates_bearer"] = "PASS"
            except Exception:
                log.flush()
                log.seek(0)
                print("Backend diagnostic tail:", log.read()[-3000:].replace(owner, "<redacted>"), file=sys.stderr)
                raise
            finally:
                if child.poll() is None:
                    child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
                    raise RuntimeError("OAuth backend did not shut down gracefully")
                log.seek(0)
                emitted = log.read()
                if owner in emitted:
                    raise RuntimeError("owner secret leaked into backend logs")
            assert child.returncode == 0, f"backend exit {child.returncode}"
            result["graceful_shutdown"] = "PASS"
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    arguments = parser.parse_args()
    run(arguments.binary.resolve())
