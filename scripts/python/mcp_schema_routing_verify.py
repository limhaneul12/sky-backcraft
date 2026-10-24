#!/usr/bin/env python3
"""Validate real tools/list and a synthetic host account envelope, without trading.

Run: uv run --with jsonschema==4.26.0 python scripts/python/mcp_schema_routing_verify.py
     --binary /absolute/path/to/spot-lab --out /absolute/evidence/directory

Uses fresh loopback servers, empty temporary storage and MOCK desktop access only.
The host envelope is a regression model, not a captured ChatGPT schema or router.
"""

import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

from jsonschema import Draft7Validator, Draft202012Validator
from mcp_desktop_smoke import request
from research_mcp_e2e import IsolatedServer, current_source_digest
from final_closure_verify import original_tagged_schema, schemas_match


ACCOUNTS = ("primary-fixture", "backcraft-fixture")


def compose_host(schema):
    schema = copy.deepcopy(schema)
    schema.setdefault("properties", {})["link_id"] = {"type": "string", "enum": list(ACCOUNTS)}
    schema.setdefault("required", []).append("link_id")
    return schema


def sample(schema, root):
    """Build minimal schema-only examples; every result is independently validated."""
    if schema is True or schema == {}:
        return {}
    if schema is False:
        raise ValueError("cannot sample an unsatisfiable schema")
    if "$ref" in schema:
        target = root
        for part in schema["$ref"].removeprefix("#/").split("/"):
            target = target[part.replace("~1", "/").replace("~0", "~")]
        return sample(target, root)
    if "const" in schema:
        return schema["const"]
    if "enum" in schema:
        return schema["enum"][0]
    for keyword in ("oneOf", "anyOf"):
        if keyword in schema:
            return sample(schema[keyword][0], root)
    kind = schema.get("type")
    if isinstance(kind, list):
        kind = kind[0]
    if kind == "object":
        return {name: sample(schema["properties"][name], root) for name in schema.get("required", [])}
    if kind == "array":
        return [sample(schema["items"], root) for _ in range(schema.get("minItems", 0))]
    if kind in ("integer", "number"):
        return schema.get("minimum", 0)
    if kind == "string":
        return "2026-10-23T00:00:00Z" if schema.get("format") == "date-time" else "fixture"
    if kind == "boolean":
        return False
    if kind == "null":
        return None
    raise ValueError(f"unsupported fixture schema: {schema}")


def assert_validity(validator, arguments, expected, label):
    errors = list(validator.iter_errors(arguments))
    if bool(errors) == expected:
        raise AssertionError(f"{label}: expected valid={expected}; "
                             f"errors={[error.message for error in errors[:3]]}")


def verify_tools(tools):
    results = []
    for tool in tools:
        name, schema = tool["name"], tool["inputSchema"]
        host = compose_host(schema)
        root_unions = [key for key in ("oneOf", "anyOf", "allOf") if key in schema]
        branches = schema.get("oneOf", [schema])
        cases = 0
        strict = schema.get("additionalProperties") is False or bool(root_unions)
        for validator_type in (Draft202012Validator, Draft7Validator):
            validator_type.check_schema(schema)
            validator_type.check_schema(host)
            raw_validator, host_validator = validator_type(schema), validator_type(host)
            for branch in branches:
                args = sample(branch, schema)
                label = f"{name}/{args.get('action', 'object')}/{validator_type.__name__}"
                assert_validity(raw_validator, args, True, label)
                if strict:
                    assert_validity(raw_validator, dict(args, unexpected_schema_probe=True), False, label)
                    assert_validity(raw_validator, dict(args, link_id=ACCOUNTS[0]), False, label)
                for account in ACCOUNTS:
                    linked = dict(args, link_id=account)
                    assert_validity(host_validator, linked, True, label)
                    if strict:
                        assert_validity(host_validator, dict(linked, unexpected_schema_probe=True), False, label)
                    if "action" in args:
                        assert_validity(host_validator, dict(linked, action="nonexistent_action"), False, label)
                    for field in branch.get("required", []):
                        missing = dict(linked)
                        del missing[field]
                        assert_validity(host_validator, missing, False, f"{label}/missing:{field}")
                    for field in schema.get("properties", {}):
                        if field not in branch.get("properties", {}):
                            assert_validity(host_validator, dict(linked, **{field: None}), False,
                                            f"{label}/cross-action:{field}")
                assert_validity(host_validator, args, False, f"{label}/missing-link")
                for invalid in ("unknown-account", "", None):
                    assert_validity(host_validator, dict(args, link_id=invalid), False, label)
                # Nested strict objects must stay strict after root composition.
                for field, value in args.items():
                    if isinstance(value, dict):
                        malformed = copy.deepcopy(args)
                        malformed[field]["unexpected_schema_probe"] = True
                        if not raw_validator.is_valid(malformed):
                            assert_validity(host_validator, dict(malformed, link_id=ACCOUNTS[0]), False,
                                            f"{label}/nested:{field}")
                cases += 1
        results.append({"name": name, "root_unions": root_unions,
                        "root_additional_properties": schema.get("additionalProperties"),
                        "validated_examples": cases})
    return results


def desktop_catalog(client, token):
    url = client.url.replace("/mcp", "/desktop/mcp")
    def rpc(method, params=None):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params is not None:
            payload["params"] = params
        status, body = request(url, payload, token=token)
        if status != 200 or "error" in body:
            raise RuntimeError(f"isolated desktop {method}: HTTP {status}: {body}")
        return body["result"]
    rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "schema-only-verifier", "version": "1"}})
    return rpc("tools/list")["tools"], rpc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--before", type=Path, help="optional pre-fix tools/list for exact DTO parity")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "result.json").unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="backcraft-schema-routing-") as temporary:
        env = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "LC_CTYPE") if key in os.environ}
        token = secrets.token_hex(32)
        env.update(HOME=temporary, TMPDIR=temporary, SERVICE_DESKTOP_ACCESS_TOKEN=token)
        server = IsolatedServer(args.binary.resolve(), Path(temporary) / "data",
                                args.out / "server.log", environment=env)
        try:
            client = server.start()
            status = client.call("lab_status", {})
            if status["source_digest"] != current_source_digest():
                raise RuntimeError("binary source_digest differs from current source")
            public = client.rpc("tools/list")["tools"]
            desktop, desktop_rpc = desktop_catalog(client, token)
            catalog = {tool["name"]: tool for tool in public + desktop}
            for tool in public + desktop:
                if tool["inputSchema"] != catalog[tool["name"]]["inputSchema"]:
                    raise AssertionError(f"exposure-dependent schema: {tool['name']}")
            tools = sorted(catalog.values(), key=lambda tool: tool["name"])
            (args.out / "tools.json").write_text(json.dumps(tools, indent=2) + "\n")
            (args.out / "host-tools-model.json").write_text(json.dumps(
                {tool["name"]: compose_host(tool["inputSchema"]) for tool in tools}, indent=2) + "\n")
            schema_root = Path(temporary) / "schemas"
            subprocess.run([str(args.binary.resolve()), "schemas", "--out", str(schema_root)],
                           env=env, check=True, capture_output=True, text=True)
            cli_parity = schemas_match(client, schema_root)
            audit = verify_tools(tools)
            compared = []
            if args.before:
                for old_tool in json.loads(args.before.read_text()):
                    name = old_tool["name"]
                    if original_tagged_schema(catalog[name]["inputSchema"]) != old_tool["inputSchema"]:
                        raise AssertionError(f"original DTO schema changed: {name}")
                    compared.append(name)
            # These calls read only the fresh, empty fixture; no session or strategy is started.
            automation = desktop_rpc("tools/call", {"name": "automation_session_query", "arguments": {
                "action": "list", "offset": 0, "limit": 100}})
            if automation.get("isError"):
                raise RuntimeError("isolated MOCK desktop automation list failed")
            for action in ("runs", "jobs"):
                client.call("history_query", {"action": action, "limit": 100})
            result = {"status": "PASS", "source_digest": status["source_digest"],
                      "tools": len(tools), "public_tools": len(public), "desktop_tools": len(desktop),
                      "root_union_tools": sum(bool(item["root_unions"]) for item in audit),
                      "validators": ["Draft202012Validator", "Draft7Validator"],
                      "schema_examples": sum(item["validated_examples"] for item in audit),
                      "exact_before_schema_parity_tools": len(compared),
                      "cli_schema_parity": cli_parity,
                      "unchanged_open_empty_inputs": [item["name"] for item in audit
                          if item["root_additional_properties"] is None and not item["root_unions"]],
                      "host_envelope": "SYNTHETIC_FIXTURE_ONLY", "chatgpt": "NOT_VERIFIED_IN_CHATGPT",
                      "audit": audit}
        finally:
            server.stop()
    (args.out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key not in ("audit", "cli_schema_parity")}))


if __name__ == "__main__":
    main()
