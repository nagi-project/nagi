from __future__ import annotations

from pytest_mock import MockerFixture


class TestMcpCommand:
    """Tests for `nagi mcp` CLI command."""

    def test_mcp_default_readonly(self, mocker: MockerFixture) -> None:
        mock_run = mocker.patch("nagi_cli.mcp.run_stdio")
        from click.testing import CliRunner

        from nagi_cli.commands.mcp import mcp

        runner = CliRunner()
        runner.invoke(mcp, [])
        mock_run.assert_called_once_with(allow_sync=False)

    def test_mcp_allow_sync(self, mocker: MockerFixture) -> None:
        mock_run = mocker.patch("nagi_cli.mcp.run_stdio")
        from click.testing import CliRunner

        from nagi_cli.commands.mcp import mcp

        runner = CliRunner()
        runner.invoke(mcp, ["--allow-sync"])
        mock_run.assert_called_once_with(allow_sync=True)
