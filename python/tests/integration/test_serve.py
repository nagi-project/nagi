from __future__ import annotations

import json
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from tests.integration.helper import (
    NOOP_SYNC,
    SIMPLE_ASSET,
    compile_project,
    run_nagi,
    write_duckdb_project,
)

pytestmark = pytest.mark.integration

SERVE_TIMEOUT = 60

PASS_CHECK_CONDITIONS = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Conditions\n"
    "metadata:\n"
    "  name: pass-check\n"
    "spec:\n"
    "  - name: ok\n"
    "    type: Command\n"
    "    run: ['true']\n"
    "    interval: 5s\n"
)


def _init_nagi_dir(project: Path) -> None:
    from nagi_cli._nagi_core import init_workspace

    init_workspace(str(project), str(project / ".nagi"))


def _start_serve(
    project: Path,
    *,
    extra_args: list[str] | None = None,
) -> subprocess.Popen[bytes]:
    args = [
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
    ]
    if extra_args:
        args.extend(extra_args)
    return subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=project,
        start_new_session=True,
    )


def _wait_for_cache(
    proc: subprocess.Popen[bytes],
    cache_dir: Path,
    *,
    asset_name: str | None = None,
    timeout: int = SERVE_TIMEOUT,
) -> None:
    """Wait until cache files appear, or fail with diagnostic output."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            break
        if cache_dir.exists():
            files = list(cache_dir.glob("*.json"))
            if asset_name:
                files = [f for f in files if f.stem == asset_name]
            if files:
                return
        time.sleep(2)

    existing = list(cache_dir.glob("*")) if cache_dir.exists() else []
    _stop_serve(proc)
    pytest.fail(
        f"serve did not produce cache within {timeout}s\n"
        f"waiting_for={asset_name}\n"
        f"existing_files={[f.name for f in existing]}"
    )


def _stop_serve(proc: subprocess.Popen[bytes]) -> None:
    import os

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


def _read_cache(cache_dir: Path, asset_name: str) -> dict[str, Any]:
    path = cache_dir / f"{asset_name}.json"
    return json.loads(path.read_text())


class TestServe:
    def test_serve_halt(self, tmp_path: Path, duckdb_path: Path) -> None:
        """Halt suspends all assets."""
        project = tmp_path / "project"
        write_duckdb_project(project, duckdb_path, {"asset.yaml": SIMPLE_ASSET})
        _init_nagi_dir(project)
        compile_project(project)

        result = run_nagi(
            ["serve", "halt", "--target-dir", str(project / "target")],
            cwd=project,
        )
        assert result.returncode == 0, (
            f"halt failed:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
        assert "Halted:" in result.stdout or "already suspended" in result.stdout

    def test_serve_autosync_false(self, tmp_path: Path, duckdb_path: Path) -> None:
        """autoSync: false — evaluate only, no sync execution."""
        conditions = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Conditions\n"
            "metadata:\n"
            "  name: check\n"
            "spec:\n"
            "  - name: always-fail\n"
            "    type: Command\n"
            "    run: ['false']\n"
            "    interval: 5s\n"
        )
        asset = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: test-asset\n"
            "spec:\n"
            "  autoSync: false\n"
            "  onDrift:\n"
            "    - conditions: check\n"
            "      sync: reload\n"
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            duckdb_path,
            {
                "conditions.yaml": conditions,
                "sync.yaml": NOOP_SYNC,
                "asset.yaml": asset,
            },
        )
        _init_nagi_dir(project)
        cache_dir = project / "cache"
        proc = _start_serve(project)
        try:
            _wait_for_cache(proc, cache_dir, asset_name="test-asset")
            # Asset is evaluated but Drifted — no sync should run
            cached = _read_cache(cache_dir, "test-asset")
            assert cached["ready"] is False
            # No sync logs should exist (sync was not executed)
            logs_dir = project / ".nagi" / "logs"
            sync_logs = list(logs_dir.glob("*.log")) if logs_dir.exists() else []
            assert len(sync_logs) == 0
        finally:
            _stop_serve(proc)

    def test_serve_upstream_propagation(
        self, tmp_path: Path, duckdb_path: Path
    ) -> None:
        """Upstream Ready triggers downstream evaluation."""
        upstream = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: upstream\n"
            "spec:\n"
            "  onDrift:\n"
            "    - conditions: pass-check\n"
            "      sync: reload\n"
        )
        downstream = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: downstream\n"
            "spec:\n"
            "  upstreams:\n"
            "    - upstream\n"
            "  onDrift:\n"
            "    - conditions: pass-check\n"
            "      sync: reload\n"
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            duckdb_path,
            {
                "upstream.yaml": upstream,
                "downstream.yaml": downstream,
                "conditions.yaml": PASS_CHECK_CONDITIONS,
                "sync.yaml": NOOP_SYNC,
            },
        )
        _init_nagi_dir(project)
        cache_dir = project / "cache"
        proc = _start_serve(project)
        try:
            _wait_for_cache(proc, cache_dir, asset_name="downstream", timeout=30)
            assert (cache_dir / "upstream.json").exists()
            assert (cache_dir / "downstream.json").exists()
        finally:
            _stop_serve(proc)

    def test_serve_resume_after_halt(self, tmp_path: Path, duckdb_path: Path) -> None:
        """Halt then resume restores sync capability."""
        project = tmp_path / "project"
        write_duckdb_project(project, duckdb_path, {"asset.yaml": SIMPLE_ASSET})
        _init_nagi_dir(project)
        compile_project(project)

        # Halt
        run_nagi(
            ["serve", "halt", "--target-dir", str(project / "target")],
            cwd=project,
        )
        # Verify suspended
        suspended_dir = project / ".nagi" / "suspended"
        assert suspended_dir.exists()
        suspended_files = list(suspended_dir.glob("*"))
        assert len(suspended_files) == 1

        # Resume
        result = run_nagi(
            [
                "serve",
                "resume",
                "--select",
                "test-asset",
            ],
            cwd=project,
        )
        assert result.returncode == 0

        # Suspended file should be removed
        remaining = list(suspended_dir.glob("*"))
        assert len(remaining) == 0

    def test_serve_multiple_controllers(
        self, tmp_path: Path, duckdb_path: Path
    ) -> None:
        """Two independent asset groups run in separate controllers."""
        group_a = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: group-a\n"
            "spec:\n"
            "  onDrift:\n"
            "    - conditions: pass-check\n"
            "      sync: reload\n"
        )
        group_b = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: group-b\n"
            "spec:\n"
            "  onDrift:\n"
            "    - conditions: pass-check\n"
            "      sync: reload\n"
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            duckdb_path,
            {
                "group-a.yaml": group_a,
                "group-b.yaml": group_b,
                "conditions.yaml": PASS_CHECK_CONDITIONS,
                "sync.yaml": NOOP_SYNC,
            },
        )
        _init_nagi_dir(project)
        cache_dir = project / "cache"
        proc = _start_serve(project)
        try:
            _wait_for_cache(proc, cache_dir, asset_name="group-a", timeout=30)
            _wait_for_cache(proc, cache_dir, asset_name="group-b", timeout=30)
            assert (cache_dir / "group-a.json").exists()
            assert (cache_dir / "group-b.json").exists()
        finally:
            _stop_serve(proc)
