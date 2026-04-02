from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration.helper import (
    compile_project,
    run_nagi_json,
    write_duckdb_project,
)

pytestmark = pytest.mark.integration


class TestCompileDbtOrigin:
    """Compile with dbt Origin + DuckDB Connection via profilesDir."""

    def test_compile_output_and_artifacts(self, compiled_project: Path) -> None:
        target = compiled_project / "target"

        graph = json.loads((target / "graph.json").read_text())
        # 6 models + 3 sources = 9 assets, 7 dependency edges
        assert len(graph["nodes"]) == 9
        assert len(graph["edges"]) == 7

        assets_dir = target / "assets"
        for name in [
            "stg_customers",
            "stg_orders",
            "stg_products",
            "customers",
            "order_summary",
            "product_summary",
            "raw.raw_customers",
            "raw.raw_orders",
            "raw.raw_products",
        ]:
            assert (assets_dir / f"{name}.yaml").exists(), f"missing {name}.yaml"

    @pytest.mark.parametrize(
        ("kind", "expected_nonempty"),
        [
            pytest.param(None, ["assets", "syncs", "conditions"], id="all-kinds"),
            pytest.param("Sync", ["syncs"], id="sync-only"),
        ],
    )
    def test_ls(
        self,
        compiled_project: Path,
        kind: str | None,
        expected_nonempty: list[str],
    ) -> None:
        args = ["ls", "--target-dir", str(compiled_project / "target")]
        if kind:
            args.extend(["--kind", kind])
        output = run_nagi_json(args, cwd=compiled_project)
        assert isinstance(output, dict)
        for key in expected_nonempty:
            assert len(output[key]) > 0, f"{key} should not be empty"
        if kind:
            empty_keys = [k for k in output if k not in expected_nonempty]
            for key in empty_keys:
                assert len(output[key]) == 0, (
                    f"{key} should be empty with --kind {kind}"
                )


class TestCompileHandwritten:
    """Compile with hand-written Asset + DuckDB direct connection."""

    RESOURCES = {
        "upstream.yaml": (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: raw-customers\n"
            "spec:\n"
            "  connection: test-duckdb\n"
        ),
        "conditions.yaml": (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Conditions\n"
            "metadata:\n"
            "  name: freshness-check\n"
            "spec:\n"
            "  - name: recent-data\n"
            "    type: Freshness\n"
            "    maxAge: 8760h\n"
            "    interval: 1h\n"
            "    column: customer_id\n"
        ),
        "sync.yaml": (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: reload\n"
            "spec:\n"
            "  run:\n"
            "    type: Command\n"
            '    args: ["echo", "sync-executed"]\n'
        ),
        "asset.yaml": (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: customers\n"
            "spec:\n"
            "  tags: [daily]\n"
            "  connection: test-duckdb\n"
            "  upstreams:\n"
            "    - raw-customers\n"
            "  onDrift:\n"
            "    - conditions: freshness-check\n"
            "      sync: reload\n"
        ),
    }

    def test_compile_and_asset_files(self, tmp_path: Path, duckdb_path: Path) -> None:
        project = tmp_path / "project"
        write_duckdb_project(project, duckdb_path, self.RESOURCES)
        output = compile_project(project)
        # 2 assets (raw-customers, customers), 1 dependency edge
        assert output["nodes"] == 2
        assert output["edges"] == 1
        assert (project / "target" / "assets" / "customers.yaml").exists()
        assert (project / "target" / "assets" / "raw-customers.yaml").exists()


class TestCompileOverlayMerge:
    """User-defined Asset merges onDrift with Origin-generated Asset."""

    def test_overlay_merge_orders_on_drift(self, nagi_project: Path) -> None:
        import yaml

        # Add user-defined conditions + sync
        resources_dir = nagi_project / "resources"
        (resources_dir / "user-conditions.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Conditions\n"
            "metadata:\n"
            "  name: user-freshness\n"
            "spec:\n"
            "  - name: custom-check\n"
            "    type: Command\n"
            "    run: ['true']\n"
        )
        (resources_dir / "user-sync.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: user-sync\n"
            "spec:\n"
            "  run:\n"
            "    type: Command\n"
            '    args: ["echo", "user-sync"]\n'
        )
        # User-defined Asset overlays "customers" with beforeOrigin entry
        (resources_dir / "user-asset.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: customers\n"
            "spec:\n"
            "  onDrift:\n"
            "    - conditions: user-freshness\n"
            "      sync: user-sync\n"
            "      mergePosition: beforeOrigin\n"
        )

        compile_project(nagi_project)

        compiled = yaml.safe_load(
            (nagi_project / "target" / "assets" / "customers.yaml").read_text()
        )
        on_drift = compiled["spec"]["onDrift"]
        conditions_refs = [e["conditionsRef"] for e in on_drift]

        # beforeOrigin entry comes first, then Origin-generated
        assert conditions_refs[0] == "user-freshness"
        assert "dbt-tests-customers" in conditions_refs
        assert conditions_refs.index("user-freshness") < conditions_refs.index(
            "dbt-tests-customers"
        )


class TestCompileFailure:
    def test_empty_resources_dir_returns_zero_nodes(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        (project / "nagi.yaml").write_text("resourcesDir: resources\n")
        (project / "resources").mkdir()

        output = run_nagi_json(
            [
                "compile",
                "--resources-dir",
                str(project / "resources"),
                "--target-dir",
                str(project / "target"),
            ],
            cwd=project,
        )
        assert isinstance(output, dict)
        assert output["nodes"] == 0
        assert output["edges"] == 0
