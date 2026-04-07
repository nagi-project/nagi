import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from nagi_cli.commands.evaluate import evaluate
from tests.helper import (
    ASSET_NAME,
    FRESHNESS_COLUMN,
    FRESHNESS_INTERVAL,
    FRESHNESS_MAX_AGE,
    write_valid_resources,
)

MOCK_RESULTS = json.dumps(
    [
        {
            "asset": ASSET_NAME,
            "ready": True,
            "conditions": [],
        }
    ]
)


def _compile_resources(tmp_path: Path) -> Path:
    """Compile valid resources into target/ and return the target dir."""
    from nagi_cli._nagi_core import compile_assets

    resources_dir = tmp_path / "resources"
    target_dir = tmp_path / "target"
    write_valid_resources(resources_dir)
    compile_assets(str(resources_dir), str(target_dir))
    return target_dir


class TestEvaluateSuccess:
    def test_exit_code_is_zero(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_all",
            return_value=MOCK_RESULTS,
        ):
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                ],
            )
        assert result.exit_code == 0

    def test_outputs_result_json(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_all",
            return_value=MOCK_RESULTS,
        ):
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                ],
            )
        output = json.loads(result.output.strip())
        assert output[0]["asset"] == ASSET_NAME
        assert output[0]["ready"] is True

    def test_select_filters_assets(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_all",
            return_value=MOCK_RESULTS,
        ) as mock:
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                    "--select",
                    ASSET_NAME,
                ],
            )
        assert result.exit_code == 0
        mock.assert_called_once()
        assert list(mock.call_args.kwargs["selectors"]) == [ASSET_NAME]


class TestEvaluateDryRun:
    def test_dry_run_passes_flag(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_all",
            return_value="[]",
        ) as mock:
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0
        assert mock.call_args.kwargs["dry_run"] is True

    def test_dry_run_outputs_asset_list(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        result = runner.invoke(
            evaluate,
            [
                "--target-dir",
                str(target_dir),
                "--dry-run",
                "--select",
                ASSET_NAME,
            ],
        )
        output = json.loads(result.output)
        assert isinstance(output, list)
        assert len(output) == 1
        asset = output[0]
        assert asset["assetName"] == ASSET_NAME
        assert len(asset["conditions"]) == 1
        cond = asset["conditions"][0]
        assert cond["type"] == "Freshness"
        assert cond["maxAge"] == FRESHNESS_MAX_AGE
        assert cond["interval"] == FRESHNESS_INTERVAL
        assert cond["column"] == FRESHNESS_COLUMN


class TestEvaluateFailure:
    def test_missing_target_dir(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            evaluate,
            [
                "--target-dir",
                str(tmp_path / "nonexistent"),
            ],
        )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output

    def test_runtime_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_all",
            side_effect=RuntimeError("connection failed"),
        ):
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                    "--select",
                    ASSET_NAME,
                ],
            )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output
