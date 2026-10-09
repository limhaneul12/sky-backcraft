#!/usr/bin/env python3
"""Execute closure gates and embed a source-bound build acceptance receipt.

This is trusted build-operator provenance, not a signature or market-validity
claim. A receipt is emitted only after actual child commands and schema checks
succeed. All MCP listeners/data roots are disposable loopback fixtures.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from research_mcp_e2e import IsolatedServer, current_source_digest


WORKSPACE = Path(__file__).resolve().parent.parent
SMOKE = "mcp::closure_tests::real_http_mcp_closes_research_runtime_contracts"
COMMANDS = {
    "canonical_ci": "cargo xtask ci",
    "public_schema": "spot-lab schemas + TCP tools/list consistency",
    "critical_smoke": f"cargo test -p spot-lab --lib {SMOKE}",
}
SCHEMAS = {
    "policy_write": "policy-write",
    "research_suite": "research-suite-action",
    "collection_schedule": "collection-schedule-action",
    "storage_maintenance": "storage-maintenance-action",
    "result_query": "mcp-result-query",
}
OUTPUT_SCHEMAS = {
    "sweep-preflight-report", "schedule-recovery-state",
    "portfolio-result-summary", "portfolio-equity", "portfolio-allocations",
    "portfolio-rebalances", "portfolio-contributions",
    "portfolio-regime-timeline", "portfolio-regime-summary",
}


def run(arguments, log, environment):
    with log.open("w", encoding="utf-8") as output:
        result = subprocess.run(
            arguments, cwd=WORKSPACE, env=environment,
            stdout=output, stderr=subprocess.STDOUT, check=False,
        )
    if result.returncode:
        raise RuntimeError(f"gate failed ({result.returncode}); inspect {log}")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized(schema):
    if isinstance(schema, list):
        return [normalized(value) for value in schema]
    if not isinstance(schema, dict):
        return schema
    result = {
        ("$defs" if key == "definitions" else key): normalized(value)
        for key, value in schema.items() if key not in {"$schema", "title"}
    }
    if isinstance(result.get("$ref"), str):
        result["$ref"] = result["$ref"].replace("#/definitions/", "#/$defs/")
    return result


def schemas_match(client, schema_root):
    tools = client.rpc("tools/list")["tools"]
    observed = {tool["name"]: tool["inputSchema"] for tool in tools}
    for name, filename in SCHEMAS.items():
        expected = json.loads((schema_root / f"{filename}.json").read_text())
        expected["type"] = "object"
        if normalized(expected) != normalized(observed[name]):
            raise RuntimeError(f"generated schema differs from TCP tool contract: {name}")
    files = {path.stem for path in schema_root.glob("*.json")}
    if not OUTPUT_SCHEMAS <= files:
        raise RuntimeError(f"missing public output schemas: {sorted(OUTPUT_SCHEMAS - files)}")
    for path in schema_root.glob("*.json"):
        json.loads(path.read_text())
    return {
        "tools": len(tools), "compared_inputs": sorted(SCHEMAS), "schemas": len(files),
        "schema_sha256": {path.name: digest(path) for path in sorted(schema_root.glob("*.json"))},
    }


def inspect_binary(binary, evidence, schema_root=None):
    with tempfile.TemporaryDirectory(prefix="sky-backcraft-identity-") as temporary:
        server = IsolatedServer(binary, Path(temporary) / "data", evidence / "identity-server.log")
        try:
            client = server.start()
            status = client.call("lab_status", {})
            if schema_root is not None:
                compared = schemas_match(client, schema_root)
                compared["source_digest"] = status["source_digest"]
                compared["code_revision"] = status["code_revision"]
                (evidence / "public-schema.log").write_text(json.dumps(compared, sort_keys=True) + "\n")
            return status
        finally:
            server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    arguments = parser.parse_args()
    target = arguments.target_dir.resolve()
    if target.parts[:2] == ("/", "Volumes"):
        volume = Path("/Volumes") / target.parts[2]
        if volume.stat().st_dev == Path("/Volumes").stat().st_dev:
            raise RuntimeError(f"build volume is not mounted: {volume}")
    evidence = arguments.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1")
    environment.pop("SKY_BACKCRAFT_VERIFICATION_RECEIPT", None)
    environment.pop("SPOT_LAB_VERIFICATION_RECEIPT", None)
    source = current_source_digest()
    print("Running canonical CI", flush=True)
    run(["cargo", "xtask", "ci"], evidence / "canonical-ci.log", environment)
    run(["cargo", "build", "--locked", "--bin", "spot-lab"], evidence / "build-unverified.log", environment)
    binary = target / "debug" / "spot-lab"
    with tempfile.TemporaryDirectory(prefix="sky-backcraft-schemas-") as temporary:
        schema_root = Path(temporary)
        run([str(binary), "schemas", "--out", str(schema_root)], evidence / "schema-generation.log", environment)
        print("Comparing generated schemas with actual TCP MCP contracts", flush=True)
        status = inspect_binary(binary, evidence, schema_root)
        for path in schema_root.glob("*.json"):
            destination = WORKSPACE / "docs" / "schema" / path.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
    if status["implementation_status"] != "IMPLEMENTED_UNVERIFIED":
        raise RuntimeError("acceptance must start with receipt absent")
    if status["source_digest"] != source:
        raise RuntimeError("binary does not match source under verification")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=WORKSPACE).decode().strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=WORKSPACE)
    if dirty:
        revision += "-dirty"
    if status["code_revision"] != revision:
        raise RuntimeError("binary revision does not match the originating Git checkout")
    print("Running the critical real HTTP/MCP journey", flush=True)
    run(["cargo", "test", "-p", "spot-lab", "--lib", SMOKE], evidence / "critical-smoke.log", environment)
    if current_source_digest() != source:
        raise RuntimeError("source changed during acceptance; receipt refused")
    identity_fields = ["code_revision", "source_digest", "lockfile_digest", "schema_version", "engine_version", "toolchain"]
    receipt = {"receipt_version": 1, **{key: status[key] for key in identity_fields}}
    logs = {"canonical_ci": "canonical-ci.log", "public_schema": "public-schema.log", "critical_smoke": "critical-smoke.log"}
    receipt["gates"] = {
        key: {"command": COMMANDS[key], "status": "PASS", "log_sha256": digest(evidence / filename)}
        for key, filename in logs.items()
    }
    compact_receipt = json.dumps(receipt, separators=(",", ":"), sort_keys=True)
    (evidence / "verification-receipt.json").write_text(compact_receipt + "\n")
    environment["SKY_BACKCRAFT_VERIFICATION_RECEIPT"] = compact_receipt
    print("Building receipt-bearing artifact and checking its actual MCP status", flush=True)
    run(["cargo", "build", "--locked", "--bin", "spot-lab"], evidence / "build-verified.log", environment)
    final = inspect_binary(binary, evidence)
    if final["implementation_status"] != "VERIFIED" or final["verification_receipt"] != receipt:
        raise RuntimeError("final artifact did not validate the executed gate receipt")
    if any(final[key] != receipt[key] for key in identity_fields):
        raise RuntimeError("final artifact identity changed")
    if final["scope"]["execution_origin"] != "SIMULATED_ONLY" or final["scope"]["fill_observed"]:
        raise RuntimeError("simulation boundary violated")
    (evidence / "verified-artifact-status.json").write_text(json.dumps(final, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "source_digest": source, "binary": str(binary), "receipt": str(evidence / "verification-receipt.json")}), flush=True)


if __name__ == "__main__":
    main()
