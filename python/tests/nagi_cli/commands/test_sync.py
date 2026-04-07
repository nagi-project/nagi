import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.sync import sync
from tests.helper import (
    ASSET_NAME,
    write_valid_resources,
)

MOCK_PROPOSALS = json.dumps(
    [
        {
            "asset": ASSET_NAME,
            "syncType": "sync",
            "stages": ["pre", "run"],
        }
    ]
)

MOCK_EXEC_RESULT = json.dumps({"asset": ASSET_NAME, "success": True})


def _compile_resources(tmp_path: Path) -> Path:
    from nagi_cli._nagi_core import compile_assets

    resources_dir = tmp_path / "resources"
    target_dir = tmp_path / "target"
    write_valid_resources(resources_dir)
    compile_assets(str(resources_dir), str(target_dir))
    return target_dir


class TestSyncDryRun:
    def test_dry_run_shows_proposals_without_executing(
        self,
        tmp_path: Path,
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.sync.propose_sync",
                return_value=MOCK_PROPOSALS,
            ),
            patch(
                "nagi_cli.commands.sync.execute_sync_proposal",
            ) as mock_exec,
        ):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir), "--dry-run"],
            )
        assert result.exit_code == 0
        output = json.loads(result.output.strip())
        assert output["syncType"] == "sync"
        mock_exec.assert_not_called()


class TestSyncExecution:
    def test_confirmed_sync_executes(
        self,
        tmp_path: Path,
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.sync.propose_sync",
                return_value=MOCK_PROPOSALS,
            ),
            patch(
                "nagi_cli.commands.sync.execute_sync_proposal",
                return_value=MOCK_EXEC_RESULT,
            ) as mock_exec,
        ):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir)],
                input="y\n",
            )
        assert result.exit_code == 0
        mock_exec.assert_called_once()
        args = mock_exec.call_args[0]
        assert args[1] == "sync"

    def test_auto_approve_skips_confirmation(
        self,
        tmp_path: Path,
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.sync.propose_sync",
                return_value=MOCK_PROPOSALS,
            ),
            patch(
                "nagi_cli.commands.sync.execute_sync_proposal",
                return_value=MOCK_EXEC_RESULT,
            ) as mock_exec,
        ):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir), "--auto-approve"],
            )
        assert result.exit_code == 0
        mock_exec.assert_called_once()
        assert "Run sync?" not in result.output

    def test_declined_sync_is_skipped(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.sync.propose_sync",
                return_value=MOCK_PROPOSALS,
            ),
            patch(
                "nagi_cli.commands.sync.execute_sync_proposal",
            ) as mock_exec,
        ):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir)],
                input="n\n",
            )
        assert result.exit_code == 0
        mock_exec.assert_not_called()
        assert "skipped" in result.output


class TestSyncOptions:
    @pytest.mark.parametrize(
        "extra_args, expected_selectors, expected_stages",
        [
            pytest.param([], [], None, id="no-options"),
            pytest.param(
                ["--select", ASSET_NAME],
                [ASSET_NAME],
                None,
                id="with-select",
            ),
            pytest.param(
                ["--stage", "pre,run"],
                [],
                "pre,run",
                id="with-stage",
            ),
            pytest.param(
                ["--select", ASSET_NAME, "--stage", "run"],
                [ASSET_NAME],
                "run",
                id="select-and-stage",
            ),
        ],
    )
    def test_options_passed_to_propose_sync(
        self,
        tmp_path: Path,
        extra_args: list[str],
        expected_selectors: list[str],
        expected_stages: str | None,
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(
            "nagi_cli.commands.sync.propose_sync",
            return_value="[]",
        ) as mock_propose:
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir)] + extra_args,
            )
        assert result.exit_code == 0
        kwargs = mock_propose.call_args.kwargs
        assert list(kwargs["selectors"]) == expected_selectors
        assert kwargs["sync_type"] == "sync"
        assert list(kwargs["excludes"]) == []
        assert kwargs["stages"] == expected_stages


class TestSyncFailure:
    @pytest.mark.parametrize(
        "error_source, mock_target, side_effect, input_text",
        [
            pytest.param(
                "propose",
                "nagi_cli.commands.sync.propose_sync",
                RuntimeError("proposal failed"),
                None,
                id="propose-error",
            ),
        ],
    )
    def test_error_returns_exit_code_1(
        self,
        tmp_path: Path,
        error_source: str,
        mock_target: str,
        side_effect: Exception,
        input_text: str | None,
    ) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with patch(mock_target, side_effect=side_effect):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir)],
                input=input_text,
            )
        assert result.exit_code == 1
        output = json.loads(result.output.strip().split("\n")[-1])
        assert "error" in output

    def test_execute_error_returns_exit_code_1(self, tmp_path: Path) -> None:
        target_dir = _compile_resources(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "nagi_cli.commands.sync.propose_sync",
                return_value=MOCK_PROPOSALS,
            ),
            patch(
                "nagi_cli.commands.sync.execute_sync_proposal",
                side_effect=RuntimeError("execution failed"),
            ),
        ):
            result = runner.invoke(
                sync,
                ["--target-dir", str(target_dir)],
                input="y\n",
            )
        assert result.exit_code == 1
        output = json.loads(result.output.strip().split("\n")[-1])
        assert "error" in output
