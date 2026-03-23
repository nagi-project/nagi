from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from nagi_cli.mcp import create_server


class TestCreateServer:
    """Tests for MCP server tool registration."""

    @pytest.fixture()
    def readonly_server(self) -> object:
        return create_server(allow_sync=False)

    @pytest.fixture()
    def full_server(self) -> object:
        return create_server(allow_sync=True)

    def _tool_names(self, server: object) -> set[str]:
        tools = server._tool_manager.list_tools()  # type: ignore[attr-defined]
        return {t.name for t in tools}

    def test_readonly_registers_status_and_evaluate(
        self, readonly_server: object
    ) -> None:
        names = self._tool_names(readonly_server)
        assert "nagi_status" in names
        assert "nagi_evaluate" in names

    def test_readonly_excludes_sync_tools(self, readonly_server: object) -> None:
        names = self._tool_names(readonly_server)
        assert "nagi_sync" not in names
        assert "nagi_sync" not in names

    def test_allow_sync_registers_all_tools(self, full_server: object) -> None:
        names = self._tool_names(full_server)
        assert "nagi_status" in names
        assert "nagi_evaluate" in names
        assert "nagi_sync" in names


class TestMcpToolExecution:
    """Tests for MCP tool function execution paths."""

    @patch("nagi_cli.mcp.asset_status", return_value='{"assets":[]}')
    def test_nagi_status_calls_core(self, mock_status: object) -> None:
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}  # type: ignore[attr-defined]
        fn = tools["nagi_status"].fn
        result = fn(target_dir="t", selectors=["s1"], cache_dir="/c")
        mock_status.assert_called_once_with("t", ["s1"], "/c")  # type: ignore[attr-defined]
        assert result == '{"assets":[]}'

    @patch("nagi_cli.mcp.asset_status", return_value='{"assets":[]}')
    def test_nagi_status_defaults_none_selectors(self, mock_status: object) -> None:
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}  # type: ignore[attr-defined]
        fn = tools["nagi_status"].fn
        fn(target_dir="t", selectors=None, cache_dir=None)
        mock_status.assert_called_once_with("t", [], None)  # type: ignore[attr-defined]

    @patch("nagi_cli.mcp.evaluate_all", return_value='{"results":[]}')
    def test_nagi_evaluate_calls_core(self, mock_eval: object) -> None:
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}  # type: ignore[attr-defined]
        fn = tools["nagi_evaluate"].fn
        result = fn(target_dir="t", selectors=["a"], cache_dir="/c", dry_run=True)
        mock_eval.assert_called_once_with("t", ["a"], "/c", True)  # type: ignore[attr-defined]
        assert result == '{"results":[]}'

    @patch("nagi_cli.mcp.execute_sync_proposal")
    @patch("nagi_cli.mcp.propose_sync")
    def test_nagi_sync_proposes_and_executes(
        self, mock_propose: object, mock_execute: object
    ) -> None:
        proposal = {"asset": "a", "syncType": "sync"}
        mock_propose.return_value = json.dumps([proposal])  # type: ignore[attr-defined]
        mock_execute.return_value = json.dumps({"ok": True})  # type: ignore[attr-defined]

        server = create_server(allow_sync=True)
        tools = {t.name: t for t in server._tool_manager.list_tools()}  # type: ignore[attr-defined]
        fn = tools["nagi_sync"].fn
        result = json.loads(
            fn(
                target_dir="t",
                selectors=None,
                stages=None,
                cache_dir=None,
                force=False,
            )
        )
        assert result == [{"ok": True}]

    @patch("nagi_cli.mcp.propose_sync", side_effect=RuntimeError("fail"))
    def test_nagi_sync_error_returns_json(self, mock_propose: object) -> None:
        server = create_server(allow_sync=True)
        tools = {t.name: t for t in server._tool_manager.list_tools()}  # type: ignore[attr-defined]
        fn = tools["nagi_sync"].fn
        result = json.loads(
            fn(
                target_dir="t",
                selectors=None,
                stages=None,
                cache_dir=None,
                force=False,
            )
        )
        assert "error" in result
