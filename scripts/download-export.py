#!/usr/bin/env python3
"""Receive a completed export over MCP; verify and save every complete file."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import urllib.error
import urllib.request


class Client:
    def __init__(self, url):
        self.url = url
        self.sequence = 0
        self.protocol = None
        self.last_request_at = 0.0
        initialized = self.rpc("initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "backcraft-export-receiver", "version": "1"}})
        self.protocol = initialized["protocolVersion"]
        notification = urllib.request.Request(self.url, data=json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}).encode(), headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream", "MCP-Protocol-Version": self.protocol})
        with urllib.request.urlopen(notification, timeout=30) as response:
            response.read(1024)

    def rpc(self, method, params):
        self.sequence += 1
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.protocol:
            headers["MCP-Protocol-Version"] = self.protocol
        request = urllib.request.Request(self.url, data=json.dumps({"jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params}).encode(), headers=headers)
        # Leave headroom under the server's 20 requests/second ceiling.
        time.sleep(max(0.0, 0.075 - (time.monotonic() - self.last_request_at)))
        for attempt in range(4):
            self.last_request_at = time.monotonic()
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = response.read(524289)
                break
            except urllib.error.HTTPError as error:
                if error.code != 429 or attempt == 3:
                    raise
                time.sleep(1.0)
        if len(body) > 524288:
            raise ValueError("MCP response exceeds 512 KiB")
        result = json.loads(body)
        if "error" in result:
            raise ValueError(result["error"])
        return result["result"]

    def tool(self, name, arguments):
        result = self.rpc("tools/call", {"name": name, "arguments": arguments})
        if result.get("isError"):
            raise ValueError(result["content"])
        return json.loads("".join(item["text"] for item in result["content"] if item["type"] == "text"))


def _receive_chunks(client, artifact, args, output):
    """Follow verified raw-byte cursors while streaming the pinned file to staging."""
    digest = hashlib.sha256()
    offset = 0
    while True:
        chunk = client.tool("artifact_query", args)["chunk"]
        if chunk["encoding"] != "HEX" or chunk["offset"] != offset:
            raise ValueError("invalid chunk encoding or offset")
        if chunk["artifact"]["artifact_id"] != artifact["artifact_id"] or chunk["artifact"]["sha256"] != artifact["sha256"]:
            raise ValueError("artifact identity changed during receive")
        raw = bytes.fromhex(chunk["data_hex"])
        if len(raw) != chunk["raw_bytes"] or hashlib.sha256(raw).hexdigest() != chunk["chunk_sha256"]:
            raise ValueError("chunk length/hash mismatch")
        offset += len(raw)
        if offset > artifact["bytes"]:
            raise ValueError("received more bytes than catalogued")
        output.write(raw)
        digest.update(raw)
        if chunk["next_offset"] is None:
            return offset, digest.hexdigest()
        if not raw or chunk["next_offset"] != offset:
            raise ValueError("non-progressing or discontinuous chunk cursor")
        args["offset"] = offset


def _verify_decoded_file(path, artifact):
    """Check the decoded gzip identity with a bounded streaming working set."""
    decoded_hash = hashlib.sha256()
    decoded_size = 0
    with gzip.open(path, "rb") as source:
        while block := source.read(65536):
            decoded_size += len(block)
            if decoded_size > 512 * 1024 * 1024:
                raise ValueError("decoded artifact exceeds 512 MiB")
            decoded_hash.update(block)
    if decoded_size != artifact["uncompressed_bytes"] or decoded_hash.hexdigest() != artifact["uncompressed_sha256"]:
        raise ValueError("decoded-file length/SHA-256 mismatch")


def receive(client, artifact, destination):
    """Own temporary-file lifetime and publish only a fully verified immutable file."""
    name = artifact["file_name"]
    if not name or Path(name).name != name or name in (".", ".."):
        raise ValueError("unsafe file name in artifact descriptor")
    if not artifact["complete"] or not 0 <= artifact["bytes"] <= 128 * 1024 * 1024:
        raise ValueError("artifact is incomplete or exceeds 128 MiB")
    target = destination / name
    if target.exists():
        raise FileExistsError(f"refusing to overwrite {target}")
    args = dict(artifact["retrieval"]["read_arguments"])
    if artifact["retrieval"]["tool_name"] != "artifact_query":
        raise ValueError("unexpected retrieval tool")
    args.update(artifact_id=artifact["artifact_id"], expected_sha256=artifact["sha256"], offset=0, limit=131072)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination, prefix=".receiving-", delete=False) as output:
            temporary = Path(output.name)
            offset, sha256 = _receive_chunks(client, artifact, args, output)
            output.flush()
            os.fsync(output.fileno())
        if offset != artifact["bytes"] or sha256 != artifact["sha256"]:
            raise ValueError("complete-file length/SHA-256 mismatch")
        if artifact.get("uncompressed_sha256"):
            _verify_decoded_file(temporary, artifact)
        # Atomic no-clobber publication after complete verification.
        os.link(temporary, target)
        return {"artifact_id": artifact["artifact_id"], "file": str(target), "bytes": offset, "sha256": sha256, "verified": True}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def manifest_descriptor(reference, run_id):
    """Resolve a v4 file using only the downloaded manifest, without translating IDs."""
    if reference["run_id"] != run_id:
        raise ValueError("manifest reference belongs to another run")
    name = reference["relative_path"]
    if name not in ("review.json", "ledger.json.gz"):
        raise ValueError("unexpected manifest file reference")
    return {
        "artifact_id": reference["id"], "run_id": run_id, "file_name": name,
        "media_type": reference["media_type"], "bytes": reference["bytes"],
        "sha256": reference["sha256"], "complete": reference["complete"],
        "uncompressed_bytes": reference["uncompressed_bytes"],
        "uncompressed_sha256": reference["uncompressed_sha256"],
        "retrieval": {"tool_name": "artifact_query", "read_arguments": {
            "action": "read", "artifact_id": reference["id"],
            "expected_sha256": reference["sha256"], "offset": 0, "limit": 131072,
        }},
    }


def receive_export(client, artifacts, destination, payload):
    if len(artifacts) != 3 or {a["file_name"] for a in artifacts} != {"manifest.json", "review.json", "ledger.json.gz"}:
        raise ValueError("export job must name exactly manifest, review and ledger files")
    root = next(a for a in artifacts if a["file_name"] == "manifest.json")
    files = [receive(client, root, destination)]
    manifest = json.loads((destination / "manifest.json").read_text())
    if payload.get("kind") != "EXPORT" or manifest["run_id"] != root["run_id"] or manifest["run_id"] != payload["run_id"]:
        raise ValueError("received manifest does not identify the export job's run")
    market = payload.get("market")
    if market is None:
        expected_scope = {"kind": "FULL"}
    elif market in ("KRW-BTC", "KRW-ETH", "KRW-XRP"):
        expected_scope = {"kind": "ASSET", "asset": market[4:]}
    else:
        raise ValueError("unsupported export job market")
    if manifest["scope"] != expected_scope:
        raise ValueError("received manifest scope differs from export job")
    version = manifest["schema_version"]
    if version == "spot-lab-export-v4":
        refs = manifest["artifacts"]
        if len(refs) != 2 or {r["relative_path"] for r in refs} != {"review.json", "ledger.json.gz"}:
            raise ValueError("invalid manifest reference set")
        for reference in refs:
            # Identity and hash come from the received manifest; job metadata only cross-checks.
            descriptor = manifest_descriptor(reference, manifest["run_id"])
            listed = [a for a in artifacts if a["artifact_id"] == descriptor["artifact_id"]]
            if len(listed) != 1 or any(descriptor[k] != listed[0][k] for k in (
                "run_id", "file_name", "media_type", "bytes", "sha256", "complete",
                "uncompressed_bytes", "uncompressed_sha256",
            )):
                raise ValueError("manifest reference and completed job metadata disagree; no ID substitution allowed")
            files.append(receive(client, descriptor, destination))
        return {"run_id": manifest["run_id"], "scope": manifest["scope"], "manifest_references_followed": True, "warning": None, "files": files}
    if version not in ("spot-lab-export-v1", "spot-lab-export-v2", "spot-lab-export-v3"):
        raise ValueError("unsupported export manifest version")
    # Historical bytes remain receivable, but their package-local IDs are not public lookup IDs.
    files.extend(receive(client, a, destination) for a in artifacts if a["file_name"] != "manifest.json")
    return {"run_id": manifest["run_id"], "scope": manifest["scope"], "manifest_references_followed": False,
            "warning": "Legacy manifest IDs are package-local. Received via explicit job metadata; create a new export for corrected v4 references.",
            "files": files}


def _publish_package(staging, destination):
    """Exclusively claim the output directory and link verified files without clobbering."""
    destination.mkdir(mode=0o700)  # Atomic no-replace claim, including a directory created mid-receive.
    published = []
    try:
        for source in sorted(staging.iterdir()):
            target = destination / source.name
            os.link(source, target)
            published.append((target, source.stat()))
    except BaseException:
        for target, original in reversed(published):
            try:
                current = target.lstat()
                if (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino):
                    target.unlink()
            except FileNotFoundError:
                pass
        try:
            destination.rmdir()
        except OSError:
            # Preserve other names/replacements observed during best-effort rollback.
            pass
        raise


def receive_job(client, job, destination):
    """Stage and verify all files before exclusively claiming the client output directory."""
    artifacts = job.get("artifacts", [])
    if not artifacts:
        raise ValueError("job has no completed export files; wait for export completion")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"output directory must be new: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".backcraft-receiving-") as staging:
        result = receive_export(client, artifacts, Path(staging), job["job"]["payload"])
        _publish_package(Path(staging), destination)
    for file in result["files"]:
        file["file"] = str(destination / Path(file["file"]).name)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mcp-url", required=True)
    parser.add_argument("--job-id", required=True, help="completed export_report job ID")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    client = Client(args.mcp_url)
    job = client.tool("job_control", {"action": "get", "job_id": args.job_id})
    result = receive_job(client, job, args.output)
    print(json.dumps({"job_id": args.job_id, **result}, indent=2))


if __name__ == "__main__":
    main()
