import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nagi_cli.commands.inspect import inspect


@pytest.fixture()
def nagi_dir(tmp_path: Path) -> Path:
    return tmp_path / ".nagi"


@pytest.fixture()
def home_env(nagi_dir: Path) -> dict[str, str]:
    """Env dict that points both HOME (Unix) and USERPROFILE (Windows) to tmp."""
    parent = str(nagi_dir.parent)
    return {"HOME": parent, "USERPROFILE": parent}


@pytest.fixture()
def inspection_dir(nagi_dir: Path) -> Path:
    d = nagi_dir / "inspections" / "daily-sales"
    d.mkdir(parents=True)
    return d


def _write_inspection(
    inspection_dir: Path,
    execution_id: str,
    finished_at: str = "20260416T093000.000Z",
    changed: bool = True,
) -> Path:
    comparisons = (
        [
            {
                "type": "condition",
                "name": "freshness-24h",
                "before": {"state": "drifted", "reason": "age 30h > max 24h"},
                "after": {"state": "ready"},
            },
            {
                "type": "table row count",
                "name": "daily_sales",
                "before": 1000,
                "after": 1500,
            },
        ]
        if changed
        else [
            {
                "type": "condition",
                "name": "freshness-24h",
                "before": {"state": "ready"},
                "after": {"state": "ready"},
            },
        ]
    )
    data = {
        "schema_version": 2,
        "execution_id": execution_id,
        "asset_name": "daily-sales",
        "finished_at": "2026-04-16T09:30:00.000Z",
        "comparisons": comparisons,
        "jobs": [],
    }
    flag = "changed" if changed else "nochange"
    path = inspection_dir / f"{finished_at}_{flag}.{execution_id}.json"
    path.write_text(json.dumps(data))
    return path


class TestInspectCommand:
    def test_json_output(
        self,
        inspection_dir: Path,
        home_env: dict[str, str],
    ) -> None:
        _write_inspection(inspection_dir, "exec-001")
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--output", "json", "--no-pager"],
            catch_exceptions=False,
            env=home_env,
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["execution_id"] == "exec-001"

    def test_text_output(
        self,
        inspection_dir: Path,
        home_env: dict[str, str],
    ) -> None:
        _write_inspection(inspection_dir, "exec-001")
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--output", "text", "--no-pager"],
            catch_exceptions=False,
            env=home_env,
        )
        assert result.exit_code == 0, result.output
        assert "daily-sales" in result.output
        assert "exec-001" in result.output
        assert "condition" in result.output
        assert "freshness-24h" in result.output

    def test_limit_restricts_output(
        self,
        inspection_dir: Path,
        home_env: dict[str, str],
    ) -> None:
        for i in range(1, 6):
            _write_inspection(
                inspection_dir,
                f"exec-{i:03d}",
                finished_at=f"20260416T09300{i}.000Z",
            )
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--limit", "2", "--output", "json", "--no-pager"],
            catch_exceptions=False,
            env=home_env,
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 2
        assert data[0]["execution_id"] == "exec-004"
        assert data[1]["execution_id"] == "exec-005"

    def test_changed_only(
        self,
        inspection_dir: Path,
        home_env: dict[str, str],
    ) -> None:
        _write_inspection(
            inspection_dir,
            "exec-001",
            finished_at="20260416T093001.000Z",
            changed=True,
        )
        _write_inspection(
            inspection_dir,
            "exec-002",
            finished_at="20260416T093002.000Z",
            changed=False,
        )
        _write_inspection(
            inspection_dir,
            "exec-003",
            finished_at="20260416T093003.000Z",
            changed=True,
        )
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--changed-only", "--output", "json", "--no-pager"],
            catch_exceptions=False,
            env=home_env,
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 2
        assert data[0]["execution_id"] == "exec-001"
        assert data[1]["execution_id"] == "exec-003"

    def test_empty_asset(self, home_env: dict[str, str]) -> None:
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["nonexistent", "--output", "text", "--no-pager"],
            catch_exceptions=False,
            env=home_env,
        )
        assert result.exit_code == 0
        assert "No inspections found" in result.output
