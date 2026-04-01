"""Helpers for serve scenario tests.

Provides utilities to set up nagi projects, run serve, and query logs.db
to verify reconciliation loop behavior (evaluate/sync counts, ordering).
"""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any


def init_nagi_dir(project: Path) -> None:
    from nagi_cli._nagi_core import init_workspace

    init_workspace(str(project), str(project / ".nagi"))


def write_project(
    project_dir: Path,
    resources: dict[str, str],
) -> None:
    """Write a nagi project with the given resource YAML files."""
    project_dir.mkdir(exist_ok=True)
    resources_dir = project_dir / "resources"
    resources_dir.mkdir(exist_ok=True)

    nagi_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nnagiDir: {nagi_dir}\n"
    )

    for filename, content in resources.items():
        (resources_dir / filename).write_text(content)


def start_serve(project: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            "uv",
            "run",
            "nagi",
            "serve",
            "--resources-dir",
            str(project / "resources"),
            "--target-dir",
            str(project / "target"),
            "--cache-dir",
            str(project / "cache"),
            "--project-dir",
            str(project),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=project,
        start_new_session=True,
    )


def stop_serve(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            proc.wait(timeout=10)
        except (subprocess.TimeoutExpired, ProcessLookupError):
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)


def wait_for_asset_ready(
    project: Path,
    asset_name: str,
    *,
    timeout: int = 60,
) -> None:
    """Wait until the asset's cache file shows ready: true."""
    import json

    cache_dir = project / "cache"
    deadline = time.time() + timeout
    while time.time() < deadline:
        path = cache_dir / f"{asset_name}.json"
        if path.exists():
            data = json.loads(path.read_text())
            if data.get("ready") is True:
                return
        time.sleep(1)
    raise TimeoutError(f"{asset_name} did not become ready within {timeout}s")


def wait_for_sync_count(
    project: Path,
    asset_name: str,
    expected: int,
    *,
    timeout: int = 60,
) -> None:
    """Wait until sync_logs has at least `expected` entries for the asset."""
    db_path = project / ".nagi" / "logs.db"
    deadline = time.time() + timeout
    while time.time() < deadline:
        if db_path.exists():
            count = query_sync_count(project, asset_name)
            if count >= expected:
                return
        time.sleep(1)
    actual = query_sync_count(project, asset_name) if db_path.exists() else 0
    raise TimeoutError(
        f"{asset_name}: expected {expected} syncs, got {actual} within {timeout}s"
    )


def query_sync_count(project: Path, asset_name: str) -> int:
    """Count sync executions for an asset in logs.db."""
    db_path = project / ".nagi" / "logs.db"
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT execution_id) FROM sync_logs WHERE asset_name = ?",
            (asset_name,),
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def query_evaluate_count(project: Path, asset_name: str) -> int:
    """Count evaluate executions for an asset in logs.db."""
    db_path = project / ".nagi" / "logs.db"
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT evaluation_id) FROM evaluate_logs "
            "WHERE asset_name = ?",
            (asset_name,),
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def query_sync_assets_in_order(project: Path) -> list[str]:
    """Return asset names in sync execution order."""
    db_path = project / ".nagi" / "logs.db"
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT asset_name FROM sync_logs WHERE stage = 'run' ORDER BY started_at"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def read_cache(project: Path, asset_name: str) -> dict[str, Any]:
    import json

    path = project / "cache" / f"{asset_name}.json"
    return json.loads(path.read_text())


# ── YAML builders ──────────────────────────────────────────────────────


def asset_yaml(
    name: str,
    *,
    upstreams: list[str] | None = None,
    conditions: str | None = None,
    sync: str = "reload",
    auto_sync: bool = True,
) -> str:
    """Generate Asset YAML."""
    lines = [
        "apiVersion: nagi.io/v1alpha1",
        "kind: Asset",
        "metadata:",
        f"  name: {name}",
        "spec:",
    ]
    if not auto_sync:
        lines.append("  autoSync: false")
    if upstreams:
        lines.append("  upstreams:")
        for u in upstreams:
            lines.append(f"    - {u}")
    if conditions:
        lines.append("  onDrift:")
        lines.append(f"    - conditions: {conditions}")
        lines.append(f"      sync: {sync}")
    return "\n".join(lines) + "\n"


def conditions_yaml(
    name: str,
    *,
    command: str = "'true'",
    interval: str | None = None,
) -> str:
    """Generate Conditions YAML with a Command type."""
    lines = [
        "apiVersion: nagi.io/v1alpha1",
        "kind: Conditions",
        "metadata:",
        f"  name: {name}",
        "spec:",
        f"  - name: check-{name}",
        "    type: Command",
        f"    run: [{command}]",
    ]
    if interval:
        lines.append(f"    interval: {interval}")
    return "\n".join(lines) + "\n"


NOOP_SYNC = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Sync\n"
    "metadata:\n"
    "  name: reload\n"
    "spec:\n"
    "  run:\n"
    "    type: Command\n"
    '    args: ["echo", "sync"]\n'
)

SLOW_SYNC = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Sync\n"
    "metadata:\n"
    "  name: slow-reload\n"
    "spec:\n"
    "  run:\n"
    "    type: Command\n"
    '    args: ["sleep", "2"]\n'
)
