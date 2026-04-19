import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.status import status
from tests.helper import (
    ASSET_NAME,
    write_valid_resources,
)

MOCK_STATUS = json.dumps(
    [
        {
            "asset": ASSET_NAME,
            "converged": True,
            "lastSync": "2026-03-19T00:00:00Z",
        }
    ]
)


def _compile_resources(tmp_path: Path) -> Path:
    from nagi_cli._nagi_core import compile_assets

    resources_dir = tmp_path / "resources"
    target_dir = tmp_path / "target"
    write_valid_resources(resources_dir)
    compile_assets(str(resources_dir), str(target_dir))
    return target_dir


class TestStatusSuccess:
    @pytest.mark.parametrize(
        "extra_args, expected_selectors",
        [
            pytest.param([], [], id="no-options"),
            pytest.param(
                ["--select", ASSET_NAME],
                [ASSET_NAME],
                id="with-select",
            ),
        ],
    )
    def test_calls_asset_status_with_correct_args(
        self,
        tmp_path: Path,
        extra_args: list[str],
        expected_selectors: list[str],
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.status.asset_status",
            return_value=MOCK_STATUS,
        ) as mock:
            result = runner.invoke(
                status,
                ["--target-dir", str(target_dir)] + extra_args,
            )
        assert result.exit_code == 0
        mock.assert_called_once()
        kwargs = mock.call_args.kwargs
        assert list(kwargs["selectors"]) == expected_selectors
        assert list(kwargs["excludes"]) == []

    def test_outputs_result_json(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.status.asset_status",
            return_value=MOCK_STATUS,
        ):
            result = runner.invoke(
                status,
                ["--target-dir", str(target_dir)],
            )
        output = json.loads(result.output.strip())
        assert output[0]["asset"] == ASSET_NAME
        assert output[0]["converged"] is True


class TestStatusFailure:
    def test_runtime_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.status.asset_status",
            side_effect=RuntimeError("cache not found"),
        ):
            result = runner.invoke(
                status,
                ["--target-dir", str(target_dir)],
            )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output
        assert "cache not found" in output["error"]
