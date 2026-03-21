from __future__ import annotations

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
        assert "nagi_resync" not in names

    def test_allow_sync_registers_all_tools(self, full_server: object) -> None:
        names = self._tool_names(full_server)
        assert "nagi_status" in names
        assert "nagi_evaluate" in names
        assert "nagi_sync" in names
        assert "nagi_resync" in names
