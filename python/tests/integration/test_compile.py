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
            "analytics.stg_customers",
            "analytics.stg_orders",
            "analytics.stg_products",
            "analytics.customers",
            "analytics.order_summary",
            "analytics.product_summary",
            "analytics.ecommerce.customers",
            "analytics.ecommerce.orders",
            "analytics.raw.raw_products",
        ]:
            assert (assets_dir / f"{name}.yaml").exists(), f"missing {name}.yaml"

    def test_origin_prefixed_sync_and_model_name(self, compiled_project: Path) -> None:
        import yaml

        target = compiled_project / "target"
        asset = yaml.safe_load(
            (target / "assets" / "analytics.customers.yaml").read_text()
        )
        on_drift = asset["spec"]["onDrift"][0]
        # Sync name is prefixed with Origin name
        assert on_drift["syncRefName"] == "analytics-dbt-run"
        # dbt run --select uses modelName (without Origin prefix)
        sync_args = on_drift["sync"]["run"]["args"]
        select_idx = sync_args.index("--select")
        assert sync_args[select_idx + 1] == "customers"

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
        # User-defined Asset overlays "analytics.customers" with beforeOrigin entry
        (resources_dir / "user-asset.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: analytics.customers\n"
            "spec:\n"
            "  onDrift:\n"
            "    - conditions: user-freshness\n"
            "      sync: user-sync\n"
            "      mergePosition: beforeOrigin\n"
        )

        compile_project(nagi_project)

        path = nagi_project / "target" / "assets" / "analytics.customers.yaml"
        compiled = yaml.safe_load(path.read_text())
        on_drift = compiled["spec"]["onDrift"]
        conditions_refs = [e["conditionsRef"] for e in on_drift]

        # beforeOrigin entry comes first, then Origin-generated
        assert conditions_refs[0] == "user-freshness"
        assert "dbt-tests-analytics.customers" in conditions_refs
        assert conditions_refs.index("user-freshness") < conditions_refs.index(
            "dbt-tests-analytics.customers"
        )


class TestCompileMultiOrigin:
    """Compile with two dbt Origins verifies cross-project linking."""

    def test_matched_source_assets_are_suppressed(
        self, multi_origin_project: Path
    ) -> None:
        target = multi_origin_project / "target"
        asset_files = [f.stem for f in (target / "assets").glob("*.yaml")]
        assert "analytics.ecommerce.customers" not in asset_files
        assert "analytics.ecommerce.orders" not in asset_files
        assert "ecommerce.customers" in asset_files
        assert "ecommerce.orders" in asset_files

    def test_upstreams_rewired_to_upstream_model(
        self, multi_origin_project: Path
    ) -> None:
        import yaml

        target = multi_origin_project / "target"
        stg_customers = yaml.safe_load(
            (target / "assets" / "analytics.stg_customers.yaml").read_text()
        )
        upstreams = stg_customers["spec"]["upstreams"]
        assert "ecommerce.customers" in upstreams
        assert "analytics.ecommerce.customers" not in upstreams

    def test_unmatched_source_preserved(self, multi_origin_project: Path) -> None:
        target = multi_origin_project / "target"
        assert (target / "assets" / "analytics.raw.raw_products.yaml").exists()

    def test_graph_reflects_cross_project_edges(
        self, multi_origin_project: Path
    ) -> None:
        target = multi_origin_project / "target"
        graph = json.loads((target / "graph.json").read_text())
        edges = graph["edges"]
        has_cross_edge = any(
            e["from"] == "ecommerce.customers" and e["to"] == "analytics.stg_customers"
            for e in edges
        )
        assert has_cross_edge, (
            f"Expected edge ecommerce.customers → analytics.stg_customers, got: {edges}"
        )

    def test_duplicate_relation_output_errors(
        self,
        dbt_ecommerce_ready: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        """Two Origins outputting to the same Relation should cause a compile error."""
        from tests.integration.helper import (
            run_nagi,
            write_nagi_project,
            write_profiles,
        )

        tmp = tmp_path_factory.mktemp("dup")
        profiles_dir = write_profiles(tmp / "profiles", {"ecommerce": "dev"})
        # Register the same dbt project as two different Origins.
        # Both will output to the same Relations.
        project_dir = tmp / "project"
        write_nagi_project(
            project_dir,
            [
                ("origin-a", "ecommerce", dbt_ecommerce_ready, profiles_dir),
                ("origin-b", "ecommerce", dbt_ecommerce_ready, profiles_dir),
            ],
        )
        result = run_nagi(
            [
                "compile",
                "--resources-dir",
                str(project_dir / "resources"),
                "--target-dir",
                str(project_dir / "target"),
                "--yes",
            ],
            cwd=project_dir,
        )
        assert result.returncode != 0
        output = result.stdout + result.stderr
        assert "multiple Origins" in output


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
