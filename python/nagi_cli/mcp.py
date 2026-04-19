"""Nagi MCP server — exposes Nagi operations as MCP tools."""

from __future__ import annotations

import json
import logging
import sys

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from nagi_cli._nagi_core import (
    asset_status,
    evaluate_all,
    execute_sync_proposal,
    propose_sync,
)

logger = logging.getLogger(__name__)

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False)


def create_server(*, allow_sync: bool = False) -> FastMCP:
    """Build the MCP server instance.

    Args:
        allow_sync: When True, also register sync tools.
    """
    mcp = FastMCP("nagi")

    @mcp.tool(annotations=READ_ONLY)
    def nagi_status(
        target_dir: str = "target",
        selectors: list[str] | None = None,
    ) -> str:
        """Show current convergence status of assets.

        Returns JSON with evaluation results, sync logs, and suspended state.

        Args:
            target_dir: Directory containing compiled output.
            selectors: Asset selector expressions (dbt-compatible).
        """
        return asset_status(target_dir, selectors or [])

    @mcp.tool(annotations=READ_ONLY)
    def nagi_evaluate(
        target_dir: str = "target",
        selectors: list[str] | None = None,
        dry_run: bool = False,
    ) -> str:
        """Evaluate desired conditions for assets.

        Returns JSON with per-asset evaluation results (Ready/NotReady).

        Args:
            target_dir: Directory containing compiled output.
            selectors: Asset selector expressions (dbt-compatible).
            dry_run: When true, list assets without executing queries.
        """
        return evaluate_all(target_dir, selectors or [], dry_run=dry_run)

    if allow_sync:
        _register_sync_tools(mcp)

    return mcp


def _register_sync_tools(mcp: FastMCP) -> None:
    @mcp.tool(annotations=WRITE)
    def nagi_sync(
        target_dir: str = "target",
        selectors: list[str] | None = None,
        stages: str | None = None,
        force: bool = False,
    ) -> str:
        """Execute sync convergence operation for assets.

        Proposes sync plans and executes them. Returns JSON results.

        Args:
            target_dir: Directory containing compiled output.
            selectors: Asset selector expressions (dbt-compatible).
            stages: Comma-separated stages to execute (e.g. pre,run).
            force: Skip pre-flight checks.
        """
        return _run_sync("sync", target_dir, selectors, stages, force)


def _run_sync(
    sync_type: str,
    target_dir: str,
    selectors: list[str] | None,
    stages: str | None,
    force: bool,
) -> str:
    try:
        proposals = json.loads(
            propose_sync(target_dir, selectors or [], sync_type, stages=stages)
        )
        results = []
        for proposal in proposals:
            result_json = execute_sync_proposal(
                json.dumps(proposal), sync_type, stages, force
            )
            results.append(json.loads(result_json))
        return json.dumps(results)
    except (RuntimeError, json.JSONDecodeError) as e:
        return json.dumps({"error": str(e)})


def run_stdio(*, allow_sync: bool = False) -> None:
    """Start the MCP server on stdio transport."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    server = create_server(allow_sync=allow_sync)
    server.run(transport="stdio")
