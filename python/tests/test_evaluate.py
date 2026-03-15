import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from nagi_cli.commands.evaluate import evaluate
from tests.helper import ASSET_YAML

MOCK_RESULT = json.dumps(
    {
        "asset": "daily-sales",
        "ready": True,
        "conditions": [],
    }
)


class TestEvaluateSuccess:
    def test_exit_code_is_zero(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "asset.yaml"
        yaml_path.write_text(ASSET_YAML)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
        ):
            result = runner.invoke(
                evaluate,
                [str(yaml_path), "--profile", "my_project"],
            )
        assert result.exit_code == 0

    def test_outputs_result_json(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "asset.yaml"
        yaml_path.write_text(ASSET_YAML)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
        ):
            result = runner.invoke(
                evaluate,
                [str(yaml_path), "--profile", "my_project"],
            )
        output = json.loads(result.output)
        assert output["asset"] == "daily-sales"
        assert output["ready"] is True

    def test_passes_target_option(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "asset.yaml"
        yaml_path.write_text(ASSET_YAML)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            return_value=MOCK_RESULT,
        ) as mock:
            runner.invoke(
                evaluate,
                [
                    str(yaml_path),
                    "--profile",
                    "my_project",
                    "--target",
                    "dev",
                ],
            )
        mock.assert_called_once()
        _, kwargs = mock.call_args
        assert kwargs.get("target") == "dev" or mock.call_args[0][2] == "dev"


class TestEvaluateFailure:
    def test_runtime_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "asset.yaml"
        yaml_path.write_text(ASSET_YAML)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.evaluate.evaluate_asset",
            side_effect=RuntimeError("connection failed"),
        ):
            result = runner.invoke(
                evaluate,
                [str(yaml_path), "--profile", "my_project"],
            )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output

    def test_missing_yaml_file(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            evaluate,
            ["/nonexistent/path.yaml", "--profile", "my_project"],
        )
        assert result.exit_code != 0
