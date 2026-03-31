from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.integration.helper import (
    DRIFTED_ASSET,
    DRIFTED_CONDITIONS,
    NOOP_SYNC,
    compile_project,
    run_nagi_json,
    write_duckdb_project,
)

pytestmark = pytest.mark.integration


@pytest.fixture()
def drifted_project(tmp_path: Path, duckdb_path: Path) -> Path:
    project = tmp_path / "project"
    write_duckdb_project(
        project,
        duckdb_path,
        {
            "conditions.yaml": DRIFTED_CONDITIONS,
            "sync.yaml": NOOP_SYNC,
            "asset.yaml": DRIFTED_ASSET,
        },
    )
    compile_project(project)
    return project


class TestSyncDryRun:
    def test_dry_run_shows_planned_sync(self, drifted_project: Path) -> None:
        output = run_nagi_json(
            [
                "sync",
                "--target-dir",
                str(drifted_project / "target"),
                "--cache-dir",
                str(drifted_project / "cache"),
                "--dry-run",
            ],
            cwd=drifted_project,
        )
        assert output["asset"] == "test-asset"  # type: ignore[index]
        assert output["syncType"] == "sync"  # type: ignore[index]


class TestSyncExecution:
    @pytest.mark.parametrize(
        ("stdin", "expect_skipped"),
        [
            pytest.param("y\n", False, id="confirm-yes"),
            pytest.param("n\n", True, id="confirm-no"),
        ],
    )
    def test_sync_confirmation(
        self,
        drifted_project: Path,
        stdin: str,
        expect_skipped: bool,
    ) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "nagi",
                "sync",
                "--target-dir",
                str(drifted_project / "target"),
                "--cache-dir",
                str(drifted_project / "cache"),
            ],
            input=stdin,
            capture_output=True,
            text=True,
            cwd=drifted_project,
        )
        assert result.returncode == 0
        if expect_skipped:
            assert "skipped" in result.stdout
        else:
            json_lines = [
                line
                for line in result.stdout.splitlines()
                if line.strip().startswith("{")
            ]
            assert len(json_lines) >= 1

    def test_sync_failure_reports_error(
        self, tmp_path: Path, duckdb_path: Path
    ) -> None:
        """Sync with a failing command reports error."""
        failing_sync = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: reload\n"
            "spec:\n"
            "  run:\n"
            "    type: Command\n"
            '    args: ["false"]\n'
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            duckdb_path,
            {
                "conditions.yaml": DRIFTED_CONDITIONS,
                "sync.yaml": failing_sync,
                "asset.yaml": DRIFTED_ASSET,
            },
        )
        compile_project(project)
        result = subprocess.run(
            [
                "uv",
                "run",
                "nagi",
                "sync",
                "--target-dir",
                str(project / "target"),
                "--cache-dir",
                str(project / "cache"),
            ],
            input="y\n",
            capture_output=True,
            text=True,
            cwd=project,
        )
        assert result.returncode == 0
        assert '"success":false' in result.stdout.replace(" ", "")

    def test_sync_pre_and_post_stages(self, tmp_path: Path, duckdb_path: Path) -> None:
        """Sync with pre/post stages executes all three."""
        marker_dir = tmp_path / "markers"
        sync_with_stages = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: reload\n"
            "spec:\n"
            "  pre:\n"
            "    type: Command\n"
            f'    args: ["sh", "-c", "mkdir -p {marker_dir}'
            f' && touch {marker_dir}/pre.ok"]\n'
            "  run:\n"
            "    type: Command\n"
            f'    args: ["sh", "-c", "touch {marker_dir}/run.ok"]\n'
            "  post:\n"
            "    type: Command\n"
            f'    args: ["sh", "-c", "touch {marker_dir}/post.ok"]\n'
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            duckdb_path,
            {
                "conditions.yaml": DRIFTED_CONDITIONS,
                "sync.yaml": sync_with_stages,
                "asset.yaml": DRIFTED_ASSET,
            },
        )
        compile_project(project)
        subprocess.run(
            [
                "uv",
                "run",
                "nagi",
                "sync",
                "--target-dir",
                str(project / "target"),
                "--cache-dir",
                str(project / "cache"),
            ],
            input="y\n",
            capture_output=True,
            text=True,
            cwd=project,
            check=True,
        )
        assert (marker_dir / "pre.ok").exists()
        assert (marker_dir / "run.ok").exists()
        assert (marker_dir / "post.ok").exists()

    def test_sync_stage_run_only(self, drifted_project: Path) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "nagi",
                "sync",
                "--target-dir",
                str(drifted_project / "target"),
                "--cache-dir",
                str(drifted_project / "cache"),
                "--stage",
                "run",
            ],
            input="y\n",
            capture_output=True,
            text=True,
            cwd=drifted_project,
        )
        assert result.returncode == 0
