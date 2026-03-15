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
    write_valid_assets,
)

MOCK_RESULT = json.dumps(
    {
        "asset": ASSET_NAME,
        "ready": True,
        "conditions": [],
    }
)


def _compile_assets(tmp_path: Path) -> Path:
    """Compile valid assets into target/ and return the target dir."""
    from nagi_cli._nagi_core import compile_assets

    assets_dir = tmp_path / "assets"
    target_dir = tmp_path / "target"
    write_valid_assets(assets_dir)
    compile_assets(str(assets_dir), str(target_dir))
    return target_dir


class TestEvaluateSuccess:
    def test_exit_code_is_zero(self, tmp_path: Path) -> None:
        target_dir = _compile_assets(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
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
        target_dir = _compile_assets(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
        ):
            result = runner.invoke(
                evaluate,
                [
                    "--target-dir",
                    str(target_dir),
                ],
            )
        # Output may contain multiple lines (one per asset); check first line.
        first_line = result.output.strip().split("\n")[0]
        output = json.loads(first_line)
        assert output["asset"] == ASSET_NAME
        assert output["ready"] is True

    def test_select_filters_assets(self, tmp_path: Path) -> None:
        target_dir = _compile_assets(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
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
        assert mock.call_count == 1


class TestEvaluateDryRun:
    def test_dry_run_does_not_evaluate(self, tmp_path: Path) -> None:
        target_dir = _compile_assets(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
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
        mock.assert_not_called()

    def test_dry_run_outputs_asset_list(self, tmp_path: Path) -> None:
        target_dir = _compile_assets(tmp_path)

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
        assert output["dry_run"] is True
        asset_names = [a["assetName"] for a in output["assets"]]
        assert ASSET_NAME in asset_names
        # Verify condition details match YAML fields
        daily = next(a for a in output["assets"] if a["assetName"] == ASSET_NAME)
        assert len(daily["conditions"]) == 1
        cond = daily["conditions"][0]
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
        target_dir = _compile_assets(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
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
