import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.serve import resume


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


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
