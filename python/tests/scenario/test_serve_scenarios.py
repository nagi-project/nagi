"""Serve scenario tests verifying reconciliation loop behavior.

Each test corresponds to a scenario from docs/architecture/serve/scenarios.md.
Tests verify evaluate/sync execution counts and ordering by querying logs.db.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.scenario.conftest import StartServe
from tests.scenario.helper import (
    NOOP_SYNC,
    SLOW_SYNC,
    asset_yaml,
    conditions_yaml,
    query_evaluate_count,
    query_sync_assets_in_order,
    query_sync_count,
    read_cache,
    wait_for_asset_ready,
    wait_for_sync_count,
)

pytestmark = pytest.mark.scenario

QUICKSTART_DIR = Path(__file__).resolve().parents[3] / "examples" / "quickstart"


class TestQuickstart:
    """Verify the examples/quickstart project converges.

    greeting (root) → farewell (downstream).
    Each checks for a file; sync creates it.
    """

    def test_quickstart_converges(
        self, run_serve: StartServe, serve_project: Path
    ) -> None:
        resources = {}
        resources_dir = QUICKSTART_DIR / "resources"
        for path in resources_dir.iterdir():
            if path.suffix == ".yaml":
                resources[path.name] = path.read_text()

        project = run_serve(resources)

        wait_for_asset_ready(project, "farewell", timeout=30)

        assert read_cache(project, "greeting")["ready"] is True
        assert read_cache(project, "farewell")["ready"] is True

        assert query_sync_count(project, "greeting") == 1
        assert query_sync_count(project, "farewell") == 1

        order = query_sync_assets_in_order(project)
        assert order.index("greeting") < order.index("farewell")


class TestScenario1LinearChain:
    """A → B → C, all Drifted initially.

    Each asset checks for a marker file (Drifted if absent).
    Sync creates the file, so re-evaluate → Ready → propagates downstream.
    """

    def test_linear_chain_converges(
        self, run_serve: StartServe, serve_project: Path
    ) -> None:
        # Each asset checks for its own marker file
        marker_dir = serve_project / "markers"
        names = ["a", "b", "c"]
        cond_docs = []
        for name in names:
            marker = marker_dir / f"{name}.ok"
            cond_docs.append(
                "apiVersion: nagi.io/v1alpha1\n"
                "kind: Conditions\n"
                "metadata:\n"
                f"  name: check-{name}\n"
                "spec:\n"
                f"  - name: file-exists-{name}\n"
                "    type: Command\n"
                f"    run: ['test', '-f', '{marker}']\n"
                "    interval: 3s\n"
            )
        conditions = "---\n".join(cond_docs)
        sync_yaml = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: create-marker\n"
            "spec:\n"
            "  run:\n"
            "    type: Command\n"
            "    args:\n"
            "      - sh\n"
            "      - -c\n"
            f'      - "mkdir -p {marker_dir}'
            " && touch "
            f'{marker_dir}/{{{{ asset.name }}}}.ok"\n'
        )
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check-a", sync="create-marker"),
                "b.yaml": asset_yaml(
                    "b",
                    upstreams=["a"],
                    conditions="check-b",
                    sync="create-marker",
                ),
                "c.yaml": asset_yaml(
                    "c",
                    upstreams=["b"],
                    conditions="check-c",
                    sync="create-marker",
                ),
                "conditions.yaml": conditions,
                "sync.yaml": sync_yaml,
            }
        )

        wait_for_asset_ready(project, "c", timeout=30)

        assert read_cache(project, "a")["ready"] is True
        assert read_cache(project, "b")["ready"] is True
        assert read_cache(project, "c")["ready"] is True

        # Each asset synced exactly once
        assert query_sync_count(project, "a") == 1
        assert query_sync_count(project, "b") == 1
        assert query_sync_count(project, "c") == 1

        # Sync order: A before B, B before C
        order = query_sync_assets_in_order(project)
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")


class TestScenario2MultipleUpstreams:
    """A, B, C → X. A, B, C have interval. X has no interval.

    When A becomes Ready, X syncs. If B becomes Ready while X is syncing,
    B's propagation is ignored. C's propagation after X finishes triggers
    another sync. X syncs at most twice (not three times).
    """

    def test_multiple_upstreams_deduplicates(self, run_serve: StartServe) -> None:
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check-interval"),
                "b.yaml": asset_yaml("b", conditions="check-interval"),
                "c.yaml": asset_yaml("c", conditions="check-interval"),
                "x.yaml": asset_yaml(
                    "x",
                    upstreams=["a", "b", "c"],
                    conditions="check",
                ),
                "conditions.yaml": (
                    conditions_yaml("check-interval", interval="5s")
                    + "---\n"
                    + conditions_yaml("check")
                ),
                "sync.yaml": NOOP_SYNC,
            }
        )

        wait_for_asset_ready(project, "x", timeout=30)

        # Spec: X syncs at most 2 times (one propagation ignored during sync)
        x_syncs = query_sync_count(project, "x")
        assert 1 <= x_syncs <= 2


class TestScenario4Fanout:
    """A → B, A → C, A → D.

    A becomes Ready, then B, C, D sync in parallel.
    """

    def test_fanout_propagates_to_all(self, run_serve: StartServe) -> None:
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check-interval"),
                "b.yaml": asset_yaml("b", upstreams=["a"], conditions="check"),
                "c.yaml": asset_yaml("c", upstreams=["a"], conditions="check"),
                "d.yaml": asset_yaml("d", upstreams=["a"], conditions="check"),
                "conditions.yaml": (
                    conditions_yaml("check-interval", interval="5s")
                    + "---\n"
                    + conditions_yaml("check")
                ),
                "sync.yaml": NOOP_SYNC,
            }
        )

        wait_for_asset_ready(project, "b", timeout=30)
        wait_for_asset_ready(project, "c", timeout=30)
        wait_for_asset_ready(project, "d", timeout=30)

        # Each downstream asset synced exactly once
        assert query_sync_count(project, "b") == 1
        assert query_sync_count(project, "c") == 1
        assert query_sync_count(project, "d") == 1


class TestScenario5Diamond:
    """A → B → X, A → C → X.

    A becomes Ready, B and C sync. C uses a slow sync to ensure B finishes
    first: B ready → X syncs → X completes → C ready → X syncs again.
    X syncs twice because each upstream Ready transition is a valid trigger.
    The concurrent dedup case (X still syncing when the second upstream
    propagates) is covered by the unit test
    propagate_downstream_diamond_syncs_once_when_concurrent.
    """

    def test_diamond_syncs_twice_with_time_gap(self, run_serve: StartServe) -> None:
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check-interval"),
                "b.yaml": asset_yaml("b", upstreams=["a"], conditions="check"),
                "c.yaml": asset_yaml(
                    "c",
                    upstreams=["a"],
                    conditions="check",
                    sync="slow-reload",
                ),
                "x.yaml": asset_yaml("x", upstreams=["b", "c"], conditions="check"),
                "conditions.yaml": (
                    conditions_yaml("check-interval", interval="5s")
                    + "---\n"
                    + conditions_yaml("check")
                ),
                "sync.yaml": NOOP_SYNC,
                "slow-sync.yaml": SLOW_SYNC,
            }
        )

        wait_for_sync_count(project, "x", 2, timeout=30)

        assert read_cache(project, "x")["ready"] is True
        # B and C each sync once
        assert query_sync_count(project, "b") == 1
        assert query_sync_count(project, "c") == 1
        # X syncs twice: once from B's propagation, once from C's.
        # Concurrent dedup is covered by unit test
        # propagate_downstream_diamond_syncs_once_when_concurrent.
        assert query_sync_count(project, "x") == 2


class TestScenario6IntervalWithPropagation:
    """A (interval) → B (interval).

    B evaluates periodically via interval AND receives upstream propagation
    when A becomes Ready.
    """

    def test_interval_and_propagation_coexist(self, run_serve: StartServe) -> None:
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check"),
                "b.yaml": asset_yaml("b", upstreams=["a"], conditions="interval-check"),
                "conditions.yaml": (
                    conditions_yaml("check")
                    + "---\n"
                    + conditions_yaml("interval-check", interval="1s")
                ),
                "sync.yaml": NOOP_SYNC,
            }
        )

        # B should be evaluated multiple times via interval
        wait_for_asset_ready(project, "b", timeout=30)

        # Wait for at least 2 interval evaluations of B
        import time

        time.sleep(3)
        eval_count = query_evaluate_count(project, "b")
        assert eval_count >= 2, f"expected B evaluated >= 2 times, got {eval_count}"


class TestScenario7UpstreamDriftedBlocksDownstream:
    """A (Drifted) → B (interval) → C.

    While A is Drifted, B's interval evaluations are blocked.
    After A syncs and becomes Ready, B and C proceed.
    """

    def test_upstream_drifted_blocks_downstream(
        self, run_serve: StartServe, serve_project: Path
    ) -> None:
        # A: Drifted until marker file exists. Sync creates marker.
        marker_dir = serve_project / "markers"
        a_marker = marker_dir / "a.ok"
        a_conditions = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Conditions\n"
            "metadata:\n"
            "  name: check-a\n"
            "spec:\n"
            "  - name: file-exists-a\n"
            "    type: Command\n"
            f"    run: ['test', '-f', '{a_marker}']\n"
            "    interval: 3s\n"
        )
        sync_yaml = (
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Sync\n"
            "metadata:\n"
            "  name: create-marker\n"
            "spec:\n"
            "  run:\n"
            "    type: Command\n"
            "    args:\n"
            "      - sh\n"
            "      - -c\n"
            f'      - "mkdir -p {marker_dir}'
            " && touch "
            f'{marker_dir}/{{{{ asset.name }}}}.ok"\n'
        )
        project = run_serve(
            {
                "a.yaml": asset_yaml("a", conditions="check-a", sync="create-marker"),
                "b.yaml": asset_yaml(
                    "b",
                    upstreams=["a"],
                    conditions="always-pass",
                    sync="create-marker",
                ),
                "c.yaml": asset_yaml(
                    "c",
                    upstreams=["b"],
                    conditions="always-pass",
                    sync="create-marker",
                ),
                "conditions.yaml": (
                    a_conditions + "---\n" + conditions_yaml("always-pass")
                ),
                "sync.yaml": sync_yaml,
            }
        )

        wait_for_asset_ready(project, "c", timeout=30)

        # Sync order: A before B, B before C (upstream Drifted blocks downstream)
        order = query_sync_assets_in_order(project)
        assert "a" in order, f"a not in sync order: {order}"
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")

        # Each asset synced exactly once
        assert query_sync_count(project, "a") == 1
        assert query_sync_count(project, "b") == 1
        assert query_sync_count(project, "c") == 1
