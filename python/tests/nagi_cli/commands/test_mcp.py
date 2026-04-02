from __future__ import annotations

import pytest

pytest.importorskip("mcp", reason="mcp extra not installed")

from click.testing import CliRunner
from pytest_mock import MockerFixture

from nagi_cli.commands.mcp import mcp


class TestMcpCommand:
    """Tests for `nagi mcp` CLI command."""

    @pytest.mark.parametrize(
        ("args", "expected_allow_sync"),
        [
            pytest.param([], False, id="default-readonly"),
            pytest.param(["--allow-sync"], True, id="allow-sync"),
        ],
    )
    def test_allow_sync_option(
        self,
        mocker: MockerFixture,
        args: list[str],
        expected_allow_sync: bool,
    ) -> None:
        mock_run = mocker.patch("nagi_cli.mcp.run_stdio")
        runner = CliRunner()
        runner.invoke(mcp, args)
        mock_run.assert_called_once_with(allow_sync=expected_allow_sync)
