#!/usr/bin/env python3
"""E2E Storage Consistency Verification between OAuth and No-Auth MCP endpoints."""

import base64
import hashlib
from html.parser import HTMLParser
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


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


def get_oauth_token(base_url, owner_code):
    opener = urllib.request.build_opener(NoRedirect())
    headers = {
        "Host": "skybackcraft.store",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "SkyBackcraft-Consistency-E2E/1.0",
    }

    # 1. Register client
    reg_req = urllib.request.Request(
        f"{base_url}/register",
        json.dumps({
            "client_name": "Consistency E2E",
            "redirect_uris": ["http://127.0.0.1:8765/callback"],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        }).encode(),
        {**headers, "Content-Type": "application/json"},
    )
    with opener.open(reg_req) as resp:
        reg_data = json.load(resp)
    client_id = reg_data["client_id"]

    # 2. Authorize
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    resource = "https://skybackcraft.store/mcp"
    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": "http://127.0.0.1:8765/callback",
        "state": "e2e-state",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "resource": resource,
    }
    auth_req = urllib.request.Request(
        f"{base_url}/authorize?{urllib.parse.urlencode(auth_params)}",
        headers=headers,
    )
    with opener.open(auth_req) as resp:
        form_html = resp.read().decode()

    parsed = ConsentForm()
    parsed.feed(form_html)
    assert parsed.fields, "no consent form fields found"

    # 3. Approve
    approve_data = urllib.parse.urlencode({**parsed.fields, "owner_code": owner_code}).encode()
    app_req = urllib.request.Request(
        f"{base_url}/authorize/approve",
        approve_data,
        {**headers, "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        resp = opener.open(app_req)
        loc = resp.headers.get("Location")
    except urllib.error.HTTPError as err:
        loc = err.headers.get("Location")

    assert loc, "no redirect location from approve"
    qs = urllib.parse.parse_qs(urllib.parse.urlsplit(loc).query)
    code = qs["code"][0]

    # 4. Token exchange
    token_data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": "http://127.0.0.1:8765/callback",
        "code": code,
        "code_verifier": verifier,
        "resource": resource,
    }).encode()
    tok_req = urllib.request.Request(
        f"{base_url}/token",
        token_data,
        {**headers, "Content-Type": "application/x-www-form-urlencoded"},
    )
    with opener.open(tok_req) as resp:
        tokens = json.load(resp)
    return tokens["access_token"]


class McpClient:
    def __init__(self, endpoint_url, token=None):
        self.endpoint_url = endpoint_url
        self.token = token
        self._req_id = 0

    def rpc(self, method, params=None):
        self._req_id += 1
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "User-Agent": "SkyBackcraft-Consistency-E2E/1.0",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        payload = {"jsonrpc": "2.0", "id": self._req_id, "method": method}
        if params is not None:
            payload["params"] = params

        for attempt in range(5):
            req = urllib.request.Request(self.endpoint_url, json.dumps(payload).encode(), headers)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.load(resp)
            except urllib.error.HTTPError as err:
                if err.code == 429 and attempt < 4:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                err_body = err.read().decode()
                return {"http_status": err.code, "error": err_body}

    def initialize(self):
        res = self.rpc("initialize", {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "e2e-consistency-client", "version": "1.0"},
        })
        return res

    def call_tool(self, name, arguments=None):
        res = self.rpc("tools/call", {
            "name": name,
            "arguments": arguments or {},
        })
        if "result" in res:
            for item in res["result"].get("content", []):
                if item.get("type") == "text":
                    try:
                        return json.loads(item["text"])
                    except Exception:
                        return item["text"]
        return res


def run_e2e_checks(base_url="http://127.0.0.1:8130"):
    owner_code = os.environ.get("SPOT_LAB_OAUTH_OWNER_CODE", "8b8089b2e669c72cc9a9606b539766849521736f0524ba81b38952b171b62548")

    print("[*] Obtaining OAuth bearer token...")
    token = get_oauth_token(base_url, owner_code)
    print(f"[+] Obtained OAuth bearer token (prefix: {token[:12]}...)")

    oauth_client = McpClient(f"{base_url}/mcp", token=token)
    public_client = McpClient(f"{base_url}/public/mcp")

    oauth_init = oauth_client.initialize()
    public_init = public_client.initialize()
    assert "result" in oauth_init, f"OAuth init failed: {oauth_init}"
    assert "result" in public_init, f"Public init failed: {public_init}"
    print("[+] Both MCP endpoints initialized successfully.")

    results = {}

    # =========================================================================
    # E2E-01: Dataset Consistency
    # =========================================================================
    print("\n--- E2E-01: Dataset Consistency ---")
    xrp_dataset_id = "dataset-260acec9eb462d96b2cb4805ca3ef72da454dbfab0bc2162fff57e9328eb5d3d"
    oauth_dataset_id = "dataset-6c94b3fb37ca9a8ac5323146cf36c0610698397a0ec62737e4a54c79bf5b2441"

    # Query XRP dataset on OAuth and No-Auth
    oauth_xrp = oauth_client.call_tool("dataset_query", {"action": "get", "dataset_id": xrp_dataset_id, "limit": 10})
    public_xrp = public_client.call_tool("dataset_query", {"action": "get", "dataset_id": xrp_dataset_id, "limit": 10})

    assert oauth_xrp["manifest"]["id"] == xrp_dataset_id
    assert public_xrp["manifest"]["id"] == xrp_dataset_id
    assert oauth_xrp["manifest"]["semantic_digest"] == public_xrp["manifest"]["semantic_digest"]
    assert oauth_xrp["manifest"]["row_count"] == public_xrp["manifest"]["row_count"]
    assert oauth_xrp["manifest"]["coverage"] == public_xrp["manifest"]["coverage"]
    assert oauth_xrp["manifest"]["total_quality_issues"] == public_xrp["manifest"]["total_quality_issues"]
    print(f"  [XRP Dataset] ID: {xrp_dataset_id}")
    print(f"  Rows: {oauth_xrp['manifest']['row_count']}, Digest: {oauth_xrp['manifest']['semantic_digest']}")
    print(f"  Coverage: {oauth_xrp['manifest']['coverage']}")
    print(f"  Quality Issues: {oauth_xrp['manifest']['total_quality_issues']}")
    print(f"  Match: OAuth == Public: OK")

    # Query OAuth dataset on both
    oauth_ds = oauth_client.call_tool("dataset_query", {"action": "get", "dataset_id": oauth_dataset_id, "limit": 10})
    public_ds = public_client.call_tool("dataset_query", {"action": "get", "dataset_id": oauth_dataset_id, "limit": 10})

    assert oauth_ds["manifest"]["id"] == oauth_dataset_id
    assert public_ds["manifest"]["id"] == oauth_dataset_id
    assert oauth_ds["manifest"]["semantic_digest"] == public_ds["manifest"]["semantic_digest"]
    assert oauth_ds["manifest"]["row_count"] == public_ds["manifest"]["row_count"]
    assert oauth_ds["manifest"]["row_count"] == 1239
    assert oauth_ds["manifest"]["total_quality_issues"] == 0
    print(f"  [OAuth Dataset] ID: {oauth_dataset_id}")
    print(f"  Rows: {oauth_ds['manifest']['row_count']}, Digest: {oauth_ds['manifest']['semantic_digest']}")
    print(f"  Quality Issues: {oauth_ds['manifest']['total_quality_issues']}")
    print(f"  Match: OAuth == Public: OK")

    results["E2E-01_dataset_consistency"] = "PASS"

    # =========================================================================
    # E2E-02: Policy Consistency
    # =========================================================================
    print("\n--- E2E-02: Policy Consistency ---")
    s2_20_10_id = "policy-57e9660b1c8ae95ba4365d8b2e0aba04a4ea11b6c24a718945fba8fa72820f34"
    s2_55_20_id = "policy-e79c618dbdabbe229f9b55ec544b452ba0c8d6ba22a1f891d9e32b26075dac6e"

    # Query policies list on both
    oauth_policies = oauth_client.call_tool("policy_query", {"action": "list", "limit": 50})
    public_policies = public_client.call_tool("policy_query", {"action": "list", "limit": 50})

    assert len(oauth_policies["items"]) == len(public_policies["items"])
    policy_ids = [p["policy_id"] for p in oauth_policies["items"]]
    assert s2_20_10_id in policy_ids
    assert s2_55_20_id in policy_ids
    print(f"  Total Policies: {len(oauth_policies['items'])} (Match: OK)")

    # Read specific revision of S2 20/10
    head_20_10 = next(p["head"] for p in oauth_policies["items"] if p["policy_id"] == s2_20_10_id)
    oauth_rev_20_10 = oauth_client.call_tool("policy_query", {"action": "get", "reference": head_20_10})
    public_rev_20_10 = public_client.call_tool("policy_query", {"action": "get", "reference": head_20_10})

    assert oauth_rev_20_10["snapshot"]["reference"]["policy_id"] == public_rev_20_10["snapshot"]["reference"]["policy_id"]
    assert oauth_rev_20_10["snapshot"]["reference"]["revision_id"] == public_rev_20_10["snapshot"]["reference"]["revision_id"]
    assert oauth_rev_20_10["snapshot"]["definition"]["name"] == public_rev_20_10["snapshot"]["definition"]["name"]
    assert oauth_rev_20_10["snapshot"]["definition"]["program"] == public_rev_20_10["snapshot"]["definition"]["program"]
    print(f"  [S2 20/10] Name: {oauth_rev_20_10['snapshot']['definition']['name']}")
    print(f"  Revision ID: {oauth_rev_20_10['snapshot']['reference']['revision_id']}")
    print(f"  Match: OK")

    # Read specific revision of S2 55/20
    head_55_20 = next(p["head"] for p in oauth_policies["items"] if p["policy_id"] == s2_55_20_id)
    oauth_rev_55_20 = oauth_client.call_tool("policy_query", {"action": "get", "reference": head_55_20})
    public_rev_55_20 = public_client.call_tool("policy_query", {"action": "get", "reference": head_55_20})

    assert oauth_rev_55_20["snapshot"]["reference"]["policy_id"] == public_rev_55_20["snapshot"]["reference"]["policy_id"]
    assert oauth_rev_55_20["snapshot"]["reference"]["revision_id"] == public_rev_55_20["snapshot"]["reference"]["revision_id"]
    assert oauth_rev_55_20["snapshot"]["definition"]["name"] == public_rev_55_20["snapshot"]["definition"]["name"]
    assert oauth_rev_55_20["snapshot"]["definition"]["program"] == public_rev_55_20["snapshot"]["definition"]["program"]
    print(f"  [S2 55/20] Name: {oauth_rev_55_20['snapshot']['definition']['name']}")
    print(f"  Revision ID: {oauth_rev_55_20['snapshot']['reference']['revision_id']}")
    print(f"  Match: OK")

    results["E2E-02_policy_consistency"] = "PASS"

    # =========================================================================
    # E2E-03: Run Consistency
    # =========================================================================
    print("\n--- E2E-03: Run Consistency ---")
    xrp_run_id = "run-e45b2828f85248fef2f98b0356586259fd767ade75ecb91459141d31fed2ec0d"
    oauth_indep_run_id = "run-694fa6163232d46261138343af34e33cda412743c5e8ef82e98325913a57dfed"
    oauth_shared_run_id = "run-d2ec8e1fb1977b78ab617d5dba40064aba7855cbd4a782e72400c8c7ab05936a"

    # Query XRP run summary
    oauth_xrp_run = oauth_client.call_tool("result_query", {"action": "summary", "run_id": xrp_run_id})
    public_xrp_run = public_client.call_tool("result_query", {"action": "summary", "run_id": xrp_run_id})

    assert oauth_xrp_run["run_id"] == xrp_run_id
    assert public_xrp_run["run_id"] == xrp_run_id
    assert oauth_xrp_run["plan_id"] == public_xrp_run["plan_id"]
    assert oauth_xrp_run["validations"] == public_xrp_run["validations"]
    assert oauth_xrp_run["models"] == public_xrp_run["models"]
    print(f"  [XRP Run] ID: {xrp_run_id}")
    print(f"  Plan ID: {oauth_xrp_run['plan_id']}")
    print(f"  Models: {len(oauth_xrp_run['models'])}, Validations: {len(oauth_xrp_run['validations'])} (Match: OK)")

    # Query OAuth Independent Models run summary
    oauth_indep_run = oauth_client.call_tool("result_query", {"action": "summary", "run_id": oauth_indep_run_id})
    public_indep_run = public_client.call_tool("result_query", {"action": "summary", "run_id": oauth_indep_run_id})

    assert oauth_indep_run["run_id"] == oauth_indep_run_id
    assert public_indep_run["run_id"] == oauth_indep_run_id
    assert oauth_indep_run["plan_id"] == public_indep_run["plan_id"]
    assert oauth_indep_run["validations"] == public_indep_run["validations"]
    assert len(oauth_indep_run["models"]) == 6
    print(f"  [OAuth Indep Run] ID: {oauth_indep_run_id}")
    print(f"  Models: {len(oauth_indep_run['models'])}, Validations: {len(oauth_indep_run['validations'])} (Match: OK)")

    # Query OAuth Shared Capital portfolio summary
    oauth_shared_run = oauth_client.call_tool("result_query", {"action": "portfolio_summary", "run_id": oauth_shared_run_id})
    public_shared_run = public_client.call_tool("result_query", {"action": "portfolio_summary", "run_id": oauth_shared_run_id})
    assert oauth_shared_run["status"] == "AVAILABLE"
    assert public_shared_run["status"] == "AVAILABLE"
    assert oauth_shared_run["data"]["run_id"] == oauth_shared_run_id
    assert public_shared_run["data"]["run_id"] == oauth_shared_run_id
    assert oauth_shared_run["data"]["terminal_equity"] == public_shared_run["data"]["terminal_equity"]
    print(f"  [OAuth Shared Capital Run] ID: {oauth_shared_run_id}")
    print(f"  Terminal Equity: {oauth_shared_run['data']['terminal_equity']} (Match: OK)")

    results["E2E-03_run_consistency"] = "PASS"

    # =========================================================================
    # E2E-04: Research Suite Consistency
    # =========================================================================
    print("\n--- E2E-04: Research Suite Consistency ---")
    suite_id = "suite-403ce8beb9e9800ffdd861dcd4b2068f9c86153f306f83d8c5255b66801a03e1"

    oauth_suite = oauth_client.call_tool("research_suite", {"action": "get", "suite_id": suite_id})
    public_suite = public_client.call_tool("research_suite", {"action": "get", "suite_id": suite_id})

    assert oauth_suite["id"] == suite_id
    assert public_suite["id"] == suite_id
    assert oauth_suite["status"] == "completed"
    assert public_suite["status"] == "completed"
    assert oauth_suite["planned_runs"] == public_suite["planned_runs"]
    assert oauth_suite["completed_runs"] == public_suite["completed_runs"]
    assert oauth_suite["input_digest"] == public_suite["input_digest"]
    assert len(oauth_suite["selected_folds"]) == len(public_suite["selected_folds"])
    print(f"  [Research Suite] ID: {suite_id}")
    print(f"  Status: {oauth_suite['status']}, Planned Runs: {oauth_suite['planned_runs']}, Completed Runs: {oauth_suite['completed_runs']}")
    print(f"  Selected Folds: {len(oauth_suite['selected_folds'])} (Match: OK)")

    results["E2E-04_suite_consistency"] = "PASS"

    # =========================================================================
    # E2E-05: Collection Schedule Consistency
    # =========================================================================
    print("\n--- E2E-05: Collection Schedule Consistency ---")
    schedule_id = "schedule-a71b2ba5240e29594cc40ee4d397bdb4d12d8dff9c2dbc2926b98512e5b45fa0"

    oauth_sched = oauth_client.call_tool("collection_schedule", {"action": "get", "schedule_id": schedule_id})
    public_sched = public_client.call_tool("collection_schedule", {"action": "get", "schedule_id": schedule_id})

    assert oauth_sched["id"] == schedule_id
    assert public_sched["id"] == schedule_id
    assert oauth_sched["status"] == "active"
    assert public_sched["status"] == "active"
    assert oauth_sched["request"]["markets"] == ["KRW-XRP"]
    assert oauth_sched["request"]["cadence_seconds"] == 7200
    assert oauth_sched["last_success_boundary"] == public_sched["last_success_boundary"]
    assert oauth_sched["next_action_at"] == public_sched["next_action_at"]
    print(f"  [Collection Schedule] ID: {schedule_id}")
    print(f"  Markets: {oauth_sched['request']['markets']}, Status: {oauth_sched['status']}, Cadence: {oauth_sched['request']['cadence_seconds']}s")
    print(f"  Last Success Boundary: {oauth_sched['last_success_boundary']}")
    print(f"  Next Action: {oauth_sched['next_action_at']}")
    print(f"  Match: OAuth == Public: OK")

    results["E2E-05_schedule_consistency"] = "PASS"

    # =========================================================================
    # E2E-06: Write Visibility & Read-only enforcement
    # =========================================================================
    print("\n--- E2E-06: Write Visibility & Read-only Enforcement ---")
    test_request_id = f"req-e2e-visibility-{int(time.time())}"
    policy_def = {
        "schema_version": "1.0",
        "name": "E2E Write Visibility Test Policy",
        "description": "Validation test policy for write visibility across endpoints",
        "program": {
            "kind": "BUILTIN",
            "strategy": {
                "kind": "S2",
                "entry_length": 35,
                "exit_length": 15,
            },
        },
    }

    # 1. Create policy via OAuth MCP
    oauth_created = oauth_client.call_tool("policy_write", {
        "action": "create",
        "request_id": test_request_id,
        "definition": policy_def,
    })
    print(f"  [OAuth MCP] Policy create response: {oauth_created}")
    created_ref = oauth_created["snapshot"]["reference"]
    created_policy_id = created_ref["policy_id"]
    created_revision_id = created_ref["revision_id"]

    # 2. Read back immediately on No-Auth MCP
    public_pol = public_client.call_tool("policy_query", {
        "action": "get",
        "reference": created_ref,
    })
    assert public_pol["snapshot"]["reference"]["policy_id"] == created_policy_id
    assert public_pol["snapshot"]["reference"]["revision_id"] == created_revision_id
    assert public_pol["snapshot"]["definition"]["name"] == policy_def["name"]
    print(f"  [Public MCP] Immediate Read-Back: Policy {created_policy_id} rev {created_revision_id} found and matches!")

    # 3. Attempt mutation on Public No-Auth MCP -> Must fail closed!
    public_mutation = public_client.call_tool("policy_write", {
        "action": "create",
        "request_id": f"req-illegal-public-{int(time.time())}",
        "definition": policy_def,
    })
    print(f"  [Public MCP] Mutation Attempt: {public_mutation}")
    # Must be an error or indicate public_no_auth disabled
    assert "disabled" in str(public_mutation).lower() or "conflict" in str(public_mutation).lower() or "error" in str(public_mutation).lower()
    print("  [Public MCP] Mutation blocked cleanly as expected.")

    results["E2E-06_write_visibility"] = "PASS"

    # =========================================================================
    # E2E-07: Single Writer / Scheduler Ownership
    # =========================================================================
    print("\n--- E2E-07: Single Writer / Scheduler Ownership ---")
    status_oauth = oauth_client.call_tool("lab_status")
    status_public = public_client.call_tool("lab_status")

    assert status_oauth["source_digest"] == status_public["source_digest"]
    assert status_oauth["jobs"]["runner_available"] is True
    assert status_public["jobs"]["runner_available"] is True
    assert status_oauth["transport"]["public_no_auth"] is False
    assert status_public["transport"]["public_no_auth"] is True

    print(f"  Lab Source Digest: {status_oauth['source_digest']}")
    print(f"  Runner Available: {status_oauth['jobs']['runner_available']}")
    print(f"  OAuth public_no_auth: {status_oauth['transport']['public_no_auth']}")
    print(f"  Public public_no_auth: {status_public['transport']['public_no_auth']}")

    results["E2E-07_single_writer_ownership"] = "PASS"

    print("\n=======================================================")
    print("ALL E2E VERIFICATION CHECKS PASSED SUCCESSFULLY (7/7)!")
    print("=======================================================")
    print(json.dumps(results, indent=2))
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="E2E Consistency Checks")
    parser.add_argument("--base-url", default="http://127.0.0.1:8130")
    args = parser.parse_args()
    if not run_e2e_checks(base_url=args.base_url):
        sys.exit(1)
