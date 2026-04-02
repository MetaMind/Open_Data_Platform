"""MetaMind OpenAPI SDK Generator.

Generates typed client SDKs for Python, TypeScript, Go, and Java
from the MetaMind OpenAPI specification.

Usage:
    python3 scripts/generate_sdk.py --lang python --out ./sdk/python/
    python3 scripts/generate_sdk.py --lang all

Requires: Docker (for openapi-generator-cli) or openapi-python-client (Python only).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUPPORTED_LANGS = ["python", "typescript", "go", "java"]
LICENSE_HEADER = """\
/*
 * MetaMind Query Intelligence Platform
 * Copyright (c) {year} MetaMind, Inc. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Auto-generated SDK — do not edit manually.
 * Generated at: {generated_at}
 */
"""

PYTHON_LICENSE_HEADER = """\
# MetaMind Query Intelligence Platform
# Copyright (c) {year} MetaMind, Inc. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Auto-generated SDK — do not edit manually.
# Generated at: {generated_at}
"""


def fetch_openapi_spec(server_url: str | None, spec_file: str | None) -> dict:
    """Fetch OpenAPI spec from running server or local file."""
    if spec_file and Path(spec_file).exists():
        logger.info("Loading spec from file: %s", spec_file)
        with open(spec_file) as fh:
            return json.load(fh)

    url = f"{server_url or 'http://localhost:8000'}/openapi.json"
    logger.info("Fetching spec from %s", url)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        logger.error("Failed to fetch OpenAPI spec: %s", exc)
        sys.exit(1)


def _inject_license(path: Path, lang: str, generated_at: str) -> None:
    """Inject MetaMind license header into generated source files."""
    year = datetime.now(tz=timezone.utc).year
    if lang == "python":
        header = PYTHON_LICENSE_HEADER.format(year=year, generated_at=generated_at)
        ext = ".py"
    else:
        header = LICENSE_HEADER.format(year=year, generated_at=generated_at)
        ext = {"typescript": ".ts", "go": ".go", "java": ".java"}.get(lang, ".txt")

    for src_file in path.rglob(f"*{ext}"):
        try:
            content = src_file.read_text(encoding="utf-8")
            if "MetaMind Query Intelligence Platform" not in content:
                src_file.write_text(header + content, encoding="utf-8")
        except Exception as exc:
            logger.warning("Could not inject header into %s: %s", src_file, exc)


def _generate_python(spec_file: Path, out_dir: Path, spec: dict) -> list[str]:
    """Generate Python SDK using openapi-python-client."""
    files: list[str] = []
    try:
        result = subprocess.run(
            [
                "openapi-python-client", "generate",
                "--path", str(spec_file),
                "--output-path", str(out_dir),
                "--overwrite",
            ],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            logger.warning("openapi-python-client failed: %s", result.stderr[:500])
    except FileNotFoundError:
        logger.info("openapi-python-client not found; creating minimal Python stub")
        _write_python_stub(out_dir, spec)

    # Patch to re-export AsyncMetaMindClient
    _patch_python_async_client(out_dir)
    files = [str(p) for p in out_dir.rglob("*.py")]
    return files


def _write_python_stub(out_dir: Path, spec: dict) -> None:
    """Write a minimal Python async client stub when generator unavailable."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stub = '''"""MetaMind Python SDK — auto-generated stub."""
from __future__ import annotations
import httpx
from typing import Any

class AsyncMetaMindClient:
    def __init__(self, base_url: str = "http://localhost:8000",
                 token: str | None = None) -> None:
        self._base = base_url.rstrip("/")
        self._token = token

    async def query(self, sql: str, tenant_id: str = "default") -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._token}"} if self._token else {}
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base}/api/v1/query",
                json={"sql": sql, "tenant_id": tenant_id},
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
'''
    (out_dir / "metamind_client.py").write_text(stub)
    (out_dir / "__init__.py").write_text(
        "from .metamind_client import AsyncMetaMindClient\n__all__ = ['AsyncMetaMindClient']\n"
    )


def _patch_python_async_client(out_dir: Path) -> None:
    """Ensure AsyncMetaMindClient is re-exported from the package root."""
    init_path = out_dir / "__init__.py"
    patch_line = "\nfrom .metamind_client import AsyncMetaMindClient  # MetaMind patch\n"
    try:
        if init_path.exists():
            content = init_path.read_text()
            if "AsyncMetaMindClient" not in content:
                init_path.write_text(content + patch_line)
    except Exception as exc:
        logger.warning("AsyncMetaMindClient patch failed: %s", exc)


def _generate_via_docker(
    lang: str, spec_file: Path, out_dir: Path
) -> list[str]:
    """Run openapi-generator-cli via Docker."""
    generator = {
        "typescript": "typescript-fetch",
        "go": "go",
        "java": "java",
    }.get(lang, lang)

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{spec_file.parent.resolve()}:/spec",
        "-v", f"{out_dir.resolve()}:/out",
        "openapitools/openapi-generator-cli:latest", "generate",
        "-i", f"/spec/{spec_file.name}",
        "-g", generator,
        "-o", "/out",
    ]
    logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error("Docker generator failed: %s", result.stderr[:500])
            _write_stub_placeholder(out_dir, lang)
    except FileNotFoundError:
        logger.warning("Docker not available; writing placeholder stub for %s", lang)
        _write_stub_placeholder(out_dir, lang)
    except subprocess.TimeoutExpired:
        logger.error("Docker generator timed out for lang=%s", lang)
        _write_stub_placeholder(out_dir, lang)

    ext = {"typescript": ".ts", "go": ".go", "java": ".java"}.get(lang, ".txt")
    return [str(p) for p in out_dir.rglob(f"*{ext}")]


def _write_stub_placeholder(out_dir: Path, lang: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"README_{lang.upper()}.md").write_text(
        f"# MetaMind {lang.capitalize()} SDK\n\n"
        "Generated SDK placeholder — run with Docker to produce full SDK.\n"
    )


def generate(
    lang: str,
    out_root: Path,
    spec: dict,
    spec_file: Path,
) -> dict:
    """Generate SDK for one language; return manifest entry."""
    generated_at = datetime.now(tz=timezone.utc).isoformat()
    out_dir = out_root / lang
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Generating %s SDK → %s", lang, out_dir)

    if lang == "python":
        files = _generate_python(spec_file, out_dir, spec)
    else:
        files = _generate_via_docker(lang, spec_file, out_dir)

    _inject_license(out_dir, lang, generated_at)

    manifest = {
        "lang": lang,
        "generated_at": generated_at,
        "spec_version": spec.get("info", {}).get("version", "unknown"),
        "files": files,
    }
    manifest_path = out_dir / "sdk_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    logger.info("SDK manifest written to %s (%d files)", manifest_path, len(files))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MetaMind SDK Generator")
    parser.add_argument(
        "--lang",
        choices=SUPPORTED_LANGS + ["all"],
        default="python",
        help="Target language (or 'all')",
    )
    parser.add_argument("--out", default="./sdk/", help="Output root directory")
    parser.add_argument("--server", default=None, help="Server base URL")
    parser.add_argument("--spec-file", default=None, help="Local OpenAPI spec JSON file")
    args = parser.parse_args(argv)

    spec = fetch_openapi_spec(args.server, args.spec_file)
    out_root = Path(args.out)

    # Write spec to tmp file for Docker-based generators
    spec_file = out_root / "openapi_spec.json"
    spec_file.parent.mkdir(parents=True, exist_ok=True)
    spec_file.write_text(json.dumps(spec, indent=2))

    langs = SUPPORTED_LANGS if args.lang == "all" else [args.lang]
    manifests = []
    for lang in langs:
        try:
            m = generate(lang, out_root, spec, spec_file)
            manifests.append(m)
        except Exception as exc:
            logger.error("SDK generation failed for %s: %s", lang, exc)

    combined = out_root / "sdk_manifest_all.json"
    combined.write_text(json.dumps(manifests, indent=2))
    logger.info("All done. Manifests: %s", combined)
    return 0


if __name__ == "__main__":
    sys.exit(main())
