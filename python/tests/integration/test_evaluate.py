from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from tests.helper import CMD_FALSE, CMD_TRUE
from tests.integration.helper import (
    NOOP_SYNC,
    build_and_evaluate,
    compile_project,
    evaluate_project,
    run_nagi,
    run_nagi_json,
    write_duckdb_project,
)

pytestmark = pytest.mark.integration


class TestEvaluateDbtOrigin:
    """Evaluate with dbt Origin-generated assets."""

    @pytest.mark.parametrize(
        ("selector", "expected_asset"),
        [
            pytest.param(
                "analytics.customers", "analytics.customers", id="single-asset"
            ),
            pytest.param(
                "label:dbt/finance", "analytics.order_summary", id="label-selector"
            ),
        ],
    )
    def test_evaluate_select(
        self,
        compiled_project: Path,
        tmp_path: Path,
        selector: str,
        expected_asset: str,
    ) -> None:
        output = evaluate_project(
            compiled_project,
            select=selector,
        )
        asset_names = [r["assetName"] for r in output]
        assert expected_asset in asset_names

    @pytest.mark.parametrize(
        ("selector", "must_include", "must_exclude"),
        [
            pytest.param(
                "1+analytics.customers",
                ["analytics.customers", "analytics.stg_customers"],
                [],
                id="upstream-1",
            ),
            pytest.param(
                "analytics.stg_customers+",
                ["analytics.stg_customers", "analytics.customers"],
                [],
                id="downstream",
            ),
            pytest.param(
                "+analytics.stg_customers+",
                ["analytics.stg_customers", "analytics.customers"],
                [],
                id="both-directions",
            ),
        ],
    )
    def test_evaluate_select_graph(
        self,
        compiled_project: Path,
        selector: str,
        must_include: list[str],
        must_exclude: list[str],
    ) -> None:
        output = run_nagi_json(
            [
                "evaluate",
                "--target-dir",
                str(compiled_project / "target"),
                "--dry-run",
                "--select",
                selector,
            ],
            cwd=compiled_project,
        )
        assert isinstance(output, list)
        asset_names = {item["assetName"] for item in output}
        for name in must_include:
            assert name in asset_names, f"{name} not in {asset_names}"
        for name in must_exclude:
            assert name not in asset_names, f"{name} in {asset_names}"

    def test_evaluate_dry_run(self, compiled_project: Path) -> None:
        output = run_nagi_json(
            [
                "evaluate",
                "--target-dir",
                str(compiled_project / "target"),
                "--dry-run",
            ],
            cwd=compiled_project,
        )
        assert isinstance(output, list)
        for item in output:
            assert "assetName" in item
            assert "conditions" in item
        has_conditions = any(len(item["conditions"]) > 0 for item in output)
        assert has_conditions

    def test_dbt_test_conditions_ready(
        self, compiled_project: Path, tmp_path: Path
    ) -> None:
        output = evaluate_project(
            compiled_project,
            select="analytics.stg_customers",
        )
        assert len(output) == 1
        assert output[0]["ready"] is True


def _conditions_yaml(name: str, spec_body: str) -> str:
    return (
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Conditions\n"
        "metadata:\n"
        f"  name: {name}\n"
        "spec:\n" + spec_body
    )


def _asset_yaml(asset_name: str, conditions_name: str, connection: bool = True) -> str:
    conn = "  connection: test-duckdb\n" if connection else ""
    return (
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Asset\n"
        "metadata:\n"
        f"  name: {asset_name}\n"
        "spec:\n"
        f"{conn}"
        "  onDrift:\n"
        f"    - conditions: {conditions_name}\n"
        "      sync: reload\n"
    )


class TestEvaluateConditionTypes:
    """Evaluate each condition type with Ready and Drifted cases."""

    @pytest.mark.parametrize(
        ("conditions_body", "setup_sql", "expected_ready"),
        [
            pytest.param(
                "  - name: recent-data\n"
                "    type: Freshness\n"
                "    maxAge: 1h\n"
                "    interval: 1h\n"
                "    column: updated_at\n",
                "CREATE TABLE IF NOT EXISTS events "
                "(id INTEGER, updated_at TIMESTAMP); "
                "DELETE FROM events; "
                "INSERT INTO events VALUES (1, now());",
                True,
                id="freshness-ready",
            ),
            pytest.param(
                "  - name: stale-data\n"
                "    type: Freshness\n"
                "    maxAge: 1h\n"
                "    interval: 1h\n"
                "    column: updated_at\n",
                "CREATE TABLE IF NOT EXISTS events "
                "(id INTEGER, updated_at TIMESTAMP); "
                "DELETE FROM events; "
                "INSERT INTO events VALUES (1, '2020-01-01 00:00:00');",
                False,
                id="freshness-drifted",
            ),
        ],
    )
    def test_freshness(
        self,
        tmp_path: Path,
        duckdb_path: Path,
        conditions_body: str,
        setup_sql: str,
        expected_ready: bool,
    ) -> None:
        subprocess.run(
            ["duckdb", str(duckdb_path), "-c", setup_sql],
            check=True,
            capture_output=True,
        )
        result = build_and_evaluate(
            tmp_path,
            duckdb_path,
            {
                "conditions.yaml": _conditions_yaml("check", conditions_body),
                "sync.yaml": NOOP_SYNC,
                "asset.yaml": _asset_yaml("events", "check"),
            },
            asset_name="events",
        )
        assert result["ready"] is expected_ready

    @pytest.mark.parametrize(
        ("query", "expected_ready"),
        [
            pytest.param(
                "SELECT COUNT(*) > 0 FROM raw_customers", True, id="sql-ready"
            ),
            pytest.param(
                "SELECT COUNT(*) = 0 FROM raw_customers",
                False,
                id="sql-drifted",
            ),
        ],
    )
    def test_sql(
        self,
        tmp_path: Path,
        duckdb_path: Path,
        query: str,
        expected_ready: bool,
    ) -> None:
        conditions_body = f'  - name: sql-check\n    type: SQL\n    query: "{query}"\n'
        result = build_and_evaluate(
            tmp_path,
            duckdb_path,
            {
                "conditions.yaml": _conditions_yaml("check", conditions_body),
                "sync.yaml": NOOP_SYNC,
                "asset.yaml": _asset_yaml("raw_customers", "check"),
            },
            asset_name="raw_customers",
        )
        assert result["ready"] is expected_ready

    @pytest.mark.parametrize(
        ("command", "expected_ready"),
        [
            pytest.param(CMD_TRUE, True, id="command-ready"),
            pytest.param(CMD_FALSE, False, id="command-drifted"),
        ],
    )
    def test_command(
        self,
        tmp_path: Path,
        duckdb_path: Path,
        command: str,
        expected_ready: bool,
    ) -> None:
        conditions_body = (
            f"  - name: cmd-check\n    type: Command\n    run: [{command}]\n"
        )
        result = build_and_evaluate(
            tmp_path,
            duckdb_path,
            {
                "conditions.yaml": _conditions_yaml("check", conditions_body),
                "sync.yaml": NOOP_SYNC,
                "asset.yaml": _asset_yaml("test-asset", "check", connection=False),
            },
            asset_name="test-asset",
        )
        assert result["ready"] is expected_ready


class TestEvaluateConnectionFailure:
    def test_nonexistent_duckdb_path(self, tmp_path: Path) -> None:
        """Evaluate with a non-existent DuckDB path returns an error."""
        bad_db = tmp_path / "nonexistent.duckdb"
        asset = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: test-asset\n"
            "spec:\n"
            "  connection: test-duckdb\n"
            "  onDrift:\n"
            "    - conditions: check\n"
            "      sync: reload\n"
        )
        conditions = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Conditions\n"
            "metadata:\n"
            "  name: check\n"
            "spec:\n"
            "  - name: sql-check\n"
            "    type: SQL\n"
            '    query: "SELECT 1"\n'
        )
        project = tmp_path / "project"
        write_duckdb_project(
            project,
            bad_db,
            {
                "conditions.yaml": conditions,
                "sync.yaml": NOOP_SYNC,
                "asset.yaml": asset,
            },
        )
        compile_project(project)
        result = run_nagi(
            [
                "evaluate",
                "--target-dir",
                str(project / "target"),
            ],
            cwd=project,
        )
        assert result.returncode == 1
        assert "error" in result.stdout
