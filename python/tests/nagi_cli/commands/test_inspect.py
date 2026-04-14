import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nagi_cli.commands.inspect import inspect


@pytest.fixture()
def nagi_dir(tmp_path: Path) -> Path:
    return tmp_path / ".nagi"


@pytest.fixture()
def inspection_dir(nagi_dir: Path) -> Path:
    d = nagi_dir / "inspections" / "daily-sales"
    d.mkdir(parents=True)
    return d


def _write_inspection(
    inspection_dir: Path,
    execution_id: str,
) -> Path:
    data = {
        "schema_version": 1,
        "execution_id": execution_id,
        "asset_name": "daily-sales",
        "before_sync": {
            "evaluations": [
                {
                    "name": "freshness-24h",
                    "status": {
                        "state": "drifted",
                        "reason": "age 30h",
                    },
                    "detail": {"age_hours": 30},
                }
            ],
            "physical_object": {
                "object_type": "BASE TABLE",
                "metrics": {"row_count": 1000},
            },
        },
        "after_sync": {
            "evaluations": [
                {
                    "name": "freshness-24h",
                    "status": {"state": "ready"},
                    "detail": {"age_hours": 0},
                }
            ],
            "physical_object": {
                "object_type": "BASE TABLE",
                "metrics": {"row_count": 1500},
            },
        },
        "destination_jobs": [],
    }
    path = inspection_dir / f"{execution_id}.json"
    path.write_text(json.dumps(data))
    return path


class TestInspectCommand:
    def test_json_output(
        self,
        nagi_dir: Path,
        inspection_dir: Path,
    ) -> None:
        _write_inspection(inspection_dir, "exec-001")
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--output", "json", "--no-pager"],
            catch_exceptions=False,
            env={"HOME": str(nagi_dir.parent)},
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["execution_id"] == "exec-001"

    def test_text_output(
        self,
        nagi_dir: Path,
        inspection_dir: Path,
    ) -> None:
        _write_inspection(inspection_dir, "exec-001")
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["daily-sales", "--output", "text", "--no-pager"],
            catch_exceptions=False,
            env={"HOME": str(nagi_dir.parent)},
        )
        assert result.exit_code == 0, result.output
        assert "daily-sales" in result.output
        assert "exec-001" in result.output
        assert "BASE TABLE" in result.output

    def test_empty_asset(self, nagi_dir: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            inspect,
            ["nonexistent", "--output", "text", "--no-pager"],
            catch_exceptions=False,
            env={"HOME": str(nagi_dir.parent)},
        )
        assert result.exit_code == 0
        assert "No inspections found" in result.output
