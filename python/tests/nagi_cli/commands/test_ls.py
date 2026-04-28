import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.ls import ls
from tests.helper import write_valid_resources

MOCK_LS_JSON = json.dumps(
    {
        "assets": [{"name": "daily-sales"}],
        "connections": [{"name": "my-bq"}],
    }
)
MOCK_LS_TEXT = "Assets:\n  daily-sales\nConnections:\n  my-bq"


def _compile_resources(tmp_path: Path) -> Path:
    from nagi_cli._nagi_core import compile_assets

    resources_dir = tmp_path / "resources"
    target_dir = tmp_path / "target"
    write_valid_resources(resources_dir)
    compile_assets(str(resources_dir), str(target_dir))
    return target_dir


class TestLsSuccess:
    def test_json_output_default(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.ls.list_resources",
            return_value=MOCK_LS_JSON,
        ):
            result = runner.invoke(ls, ["--target-dir", str(target_dir)])
        assert result.exit_code == 0
        output = json.loads(result.output.strip())
        assert "assets" in output

    def test_text_output_calls_format_ls_text(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.ls.list_resources",
                return_value=MOCK_LS_JSON,
            ),
            patch(
                "nagi_cli.commands.ls.format_ls_text",
                return_value=MOCK_LS_TEXT,
            ) as mock_fmt,
        ):
            result = runner.invoke(
                ls, ["--target-dir", str(target_dir), "--output", "text"]
            )
        assert result.exit_code == 0
        mock_fmt.assert_called_once_with(MOCK_LS_JSON)

    @pytest.mark.parametrize(
        "kinds, expected_kinds",
        [
            pytest.param([], [], id="no-filter"),
            pytest.param(
                ["--kind", "Asset"],
                ["Asset"],
                id="single-kind",
            ),
            pytest.param(
                ["--kind", "Asset", "--kind", "Connection"],
                ["Asset", "Connection"],
                id="multiple-kinds",
            ),
        ],
    )
    def test_kind_filter_passed_to_list_resources(
        self,
        tmp_path: Path,
        kinds: list[str],
        expected_kinds: list[str],
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.ls.list_resources",
            return_value=MOCK_LS_JSON,
        ) as mock:
            result = runner.invoke(ls, ["--target-dir", str(target_dir)] + kinds)
        assert result.exit_code == 0
        mock.assert_called_once()
        kwargs = mock.call_args.kwargs
        assert list(kwargs["kinds"]) == expected_kinds


class TestLsFailure:
    def test_runtime_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.ls.list_resources",
            side_effect=RuntimeError("target not found"),
        ):
            result = runner.invoke(ls, ["--target-dir", str(target_dir)])
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output
        assert "target not found" in output["error"]
