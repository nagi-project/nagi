from __future__ import annotations

import json

import pytest

pytest.importorskip("mcp", reason="mcp extra not installed")

from mcp.server.fastmcp import FastMCP
from pytest_mock import MockerFixture

from nagi_cli.mcp import create_server


class TestCreateServer:
    """Tests for MCP server tool registration."""

    @pytest.fixture()
    def readonly_server(self) -> FastMCP:
        return create_server(allow_sync=False)

    @pytest.fixture()
    def full_server(self) -> FastMCP:
        return create_server(allow_sync=True)

    def _tool_names(self, server: FastMCP) -> set[str]:
        tools = server._tool_manager.list_tools()
        return {t.name for t in tools}

    def test_readonly_registers_status_and_evaluate(
        self, readonly_server: FastMCP
    ) -> None:
        names = self._tool_names(readonly_server)
        assert "nagi_status" in names
        assert "nagi_evaluate" in names

    def test_readonly_excludes_sync_tools(self, readonly_server: FastMCP) -> None:
        names = self._tool_names(readonly_server)
        assert "nagi_sync" not in names

    def test_allow_sync_registers_all_tools(self, full_server: FastMCP) -> None:
        names = self._tool_names(full_server)
        assert "nagi_status" in names
        assert "nagi_evaluate" in names
        assert "nagi_sync" in names


class TestMcpToolExecution:
    """Tests for MCP tool function execution paths."""

    def test_nagi_status_calls_core(self, mocker: MockerFixture) -> None:
        mock_status = mocker.patch(
            "nagi_cli.mcp.asset_status", return_value='{"assets":[]}'
        )
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        fn = tools["nagi_status"].fn
        result = fn(target_dir="t", selectors=["s1"])
        mock_status.assert_called_once_with("t", ["s1"])
        assert result == '{"assets":[]}'

    def test_nagi_status_defaults_none_selectors(self, mocker: MockerFixture) -> None:
        mock_status = mocker.patch(
            "nagi_cli.mcp.asset_status", return_value='{"assets":[]}'
        )
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        fn = tools["nagi_status"].fn
        fn(target_dir="t", selectors=None)
        mock_status.assert_called_once_with("t", [])

    def test_nagi_evaluate_calls_core(self, mocker: MockerFixture) -> None:
        mock_eval = mocker.patch(
            "nagi_cli.mcp.evaluate_all", return_value='{"results":[]}'
        )
        server = create_server()
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        fn = tools["nagi_evaluate"].fn
        result = fn(target_dir="t", selectors=["a"], dry_run=True)
        mock_eval.assert_called_once_with("t", ["a"], dry_run=True)
        assert result == '{"results":[]}'

    def test_nagi_sync_proposes_and_executes(self, mocker: MockerFixture) -> None:
        proposal = {"asset": "a", "syncType": "sync"}
        mock_propose = mocker.patch("nagi_cli.mcp.propose_sync")
        mock_execute = mocker.patch("nagi_cli.mcp.execute_sync_proposal")
        mock_propose.return_value = json.dumps([proposal])
        mock_execute.return_value = json.dumps({"ok": True})

        server = create_server(allow_sync=True)
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        fn = tools["nagi_sync"].fn
        result = json.loads(
            fn(
                target_dir="t",
                selectors=None,
                stages=None,
                force=False,
            )
        )
        assert result == [{"ok": True}]

    def test_nagi_sync_error_returns_json(self, mocker: MockerFixture) -> None:
        mocker.patch("nagi_cli.mcp.propose_sync", side_effect=RuntimeError("fail"))
        server = create_server(allow_sync=True)
        tools = {t.name: t for t in server._tool_manager.list_tools()}
        fn = tools["nagi_sync"].fn
        result = json.loads(
            fn(
                target_dir="t",
                selectors=None,
                stages=None,
                force=False,
            )
        )
        assert "error" in result
