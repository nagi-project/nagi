import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.serve import halt, resume, serve


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestServeMain:
    """Tests for `nagi serve` main command."""

    @patch("nagi_cli.commands.serve._serve")
    def test_serve_calls_rust_core(
        self, mock_serve: MagicMock, runner: CliRunner
    ) -> None:
        runner.invoke(serve, ["--assets-dir", "a", "--target-dir", "t"])
        mock_serve.assert_called_once_with("a", "t", [], None, ".")

    @patch("nagi_cli.commands.serve._serve", side_effect=RuntimeError("fail"))
    def test_serve_error_returns_exit_code_1(
        self, mock_serve: MagicMock, runner: CliRunner
    ) -> None:
        result = runner.invoke(serve, [])
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output


class TestResumeInteractive:
    """Tests for `nagi serve resume` interactive selection."""

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_no_suspended_assets(
        self, mock_resume: MagicMock, runner: CliRunner
    ) -> None:
        mock_resume.return_value = json.dumps([])
        result = runner.invoke(resume, [])
        assert result.exit_code == 0
        assert "No suspended assets." in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_select_by_number(self, mock_resume: MagicMock, runner: CliRunner) -> None:
        mock_resume.side_effect = [
            json.dumps(["asset-a", "asset-b"]),  # list call
            json.dumps(["asset-b"]),  # resume call
        ]
        result = runner.invoke(resume, [], input="2\n")
        assert result.exit_code == 0
        assert "Resumed: asset-b" in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_select_all(self, mock_resume: MagicMock, runner: CliRunner) -> None:
        mock_resume.side_effect = [
            json.dumps(["asset-a", "asset-b"]),
            json.dumps(["asset-a", "asset-b"]),
        ]
        result = runner.invoke(resume, [], input="all\n")
        assert result.exit_code == 0
        assert "Resumed: asset-a" in result.output
        assert "Resumed: asset-b" in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_select_multiple_numbers(
        self, mock_resume: MagicMock, runner: CliRunner
    ) -> None:
        mock_resume.side_effect = [
            json.dumps(["asset-a", "asset-b", "asset-c"]),
            json.dumps(["asset-a", "asset-c"]),
        ]
        result = runner.invoke(resume, [], input="1,3\n")
        assert result.exit_code == 0
        assert "Resumed: asset-a" in result.output
        assert "Resumed: asset-c" in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_empty_input_no_selection(
        self, mock_resume: MagicMock, runner: CliRunner
    ) -> None:
        mock_resume.return_value = json.dumps(["asset-a"])
        result = runner.invoke(resume, [], input="\n")
        assert result.exit_code == 0
        assert "No assets selected." in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_out_of_range(self, mock_resume: MagicMock, runner: CliRunner) -> None:
        mock_resume.return_value = json.dumps(["asset-a"])
        result = runner.invoke(resume, [], input="5\n")
        assert result.exit_code == 0
        assert "Out of range: 5" in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_invalid_input(self, mock_resume: MagicMock, runner: CliRunner) -> None:
        mock_resume.return_value = json.dumps(["asset-a"])
        result = runner.invoke(resume, [], input="abc\n")
        assert result.exit_code == 0
        assert "Invalid input: abc" in result.output


class TestResumeWithSelectors:
    """Tests for `nagi serve resume --select`."""

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_resume_specific_assets(
        self, mock_resume: MagicMock, runner: CliRunner
    ) -> None:
        mock_resume.return_value = json.dumps(["asset-a"])
        result = runner.invoke(resume, ["--select", "asset-a"])
        assert result.exit_code == 0
        assert "Resumed: asset-a" in result.output

    @patch("nagi_cli.commands.serve._serve_resume")
    def test_no_matching_assets(
        self, mock_resume: MagicMock, runner: CliRunner
    ) -> None:
        mock_resume.return_value = json.dumps([])
        result = runner.invoke(resume, ["--select", "nonexistent"])
        assert result.exit_code == 0
        assert "No matching suspended assets found." in result.output


class TestHalt:
    """Tests for `nagi serve halt`."""

    @patch("nagi_cli.commands.serve._serve_halt")
    def test_halt_suspends_assets(
        self, mock_halt: MagicMock, runner: CliRunner
    ) -> None:
        mock_halt.return_value = json.dumps(["asset-a", "asset-b"])
        result = runner.invoke(halt, [])
        assert result.exit_code == 0
        assert "Halted: asset-a" in result.output
        assert "Halted: asset-b" in result.output
        assert "2 asset(s) halted." in result.output
        mock_halt.assert_called_once_with("target", None)

    @patch("nagi_cli.commands.serve._serve_halt")
    def test_halt_with_reason(self, mock_halt: MagicMock, runner: CliRunner) -> None:
        mock_halt.return_value = json.dumps(["asset-a"])
        result = runner.invoke(halt, ["--reason", "deploy in progress"])
        assert result.exit_code == 0
        mock_halt.assert_called_once_with("target", "deploy in progress")

    @patch("nagi_cli.commands.serve._serve_halt")
    def test_halt_all_already_suspended(
        self, mock_halt: MagicMock, runner: CliRunner
    ) -> None:
        mock_halt.return_value = json.dumps([])
        result = runner.invoke(halt, [])
        assert result.exit_code == 0
        assert "All assets are already suspended." in result.output

    @patch("nagi_cli.commands.serve._serve_halt")
    def test_halt_error(self, mock_halt: MagicMock, runner: CliRunner) -> None:
        mock_halt.side_effect = RuntimeError("compile error")
        result = runner.invoke(halt, [])
        assert result.exit_code == 1
