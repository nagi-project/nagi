from __future__ import annotations

from unittest.mock import patch


class TestMcpCommand:
    """Tests for `nagi mcp` CLI command."""

    @patch("nagi_cli.mcp.run_stdio")
    def test_mcp_default_readonly(self, mock_run: object) -> None:
        from click.testing import CliRunner

        from nagi_cli.commands.mcp import mcp

        runner = CliRunner()
        runner.invoke(mcp, [])
        mock_run.assert_called_once_with(allow_sync=False)  # type: ignore[attr-defined]

    @patch("nagi_cli.mcp.run_stdio")
    def test_mcp_allow_sync(self, mock_run: object) -> None:
        from click.testing import CliRunner

        from nagi_cli.commands.mcp import mcp

        runner = CliRunner()
        runner.invoke(mcp, ["--allow-sync"])
        mock_run.assert_called_once_with(allow_sync=True)  # type: ignore[attr-defined]
