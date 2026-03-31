from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.helper import (
    DRIFTED_ASSET,
    DRIFTED_CONDITIONS,
    NOOP_SYNC,
    SIMPLE_ASSET,
    compile_project,
    evaluate_project,
    run_nagi,
    run_nagi_json,
    write_duckdb_project,
)

pytestmark = pytest.mark.integration


class TestStatus:
    def test_status_after_evaluate(self, tmp_path: Path, duckdb_path: Path) -> None:
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
        evaluate_project(project)
        output = run_nagi_json(
            [
                "status",
                "--target-dir",
                str(project / "target"),
                "--cache-dir",
                str(project / "cache"),
            ],
            cwd=project,
        )
        assert isinstance(output, dict)
        assets = output["assets"]
        # 1 asset (test-asset)
        assert len(assets) == 1
        assert assets[0]["asset"] == "test-asset"

    def test_status_shows_suspended(self, tmp_path: Path, duckdb_path: Path) -> None:
        """Status includes suspended info after halt."""
        project = tmp_path / "project"
        write_duckdb_project(project, duckdb_path, {"asset.yaml": SIMPLE_ASSET})

        from nagi_cli._nagi_core import init_workspace

        init_workspace(str(project), str(project / ".nagi"))
        compile_project(project)

        run_nagi(
            ["serve", "halt", "--target-dir", str(project / "target")],
            cwd=project,
        )

        output = run_nagi_json(
            [
                "status",
                "--target-dir",
                str(project / "target"),
                "--cache-dir",
                str(project / "cache"),
            ],
            cwd=project,
        )
        assert isinstance(output, dict)
        assets = output["assets"]
        suspended = [a for a in assets if a.get("suspended")]
        assert len(suspended) == 1
