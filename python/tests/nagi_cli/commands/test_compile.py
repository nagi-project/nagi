import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.compile import compile
from tests.helper import ASSET_NAME, SYNC_YAML, write_valid_resources


class TestCompileSuccess:
    def test_exit_code_is_zero(self, tmp_path: Path) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "resources"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert result.exit_code == 0

    def test_output_contains_graph_summary(self, tmp_path: Path) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "resources"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        output = json.loads(result.output)
        assert output["nodes"] > 0
        assert output["edges"] > 0
        assert output["target"] == str(tmp_path / "target")

    @pytest.mark.parametrize(
        "expected_path",
        [
            "graph.json",
            f"assets/{ASSET_NAME}.yaml",
        ],
    )
    def test_creates_target_files(self, tmp_path: Path, expected_path: str) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "resources"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert (tmp_path / "target" / expected_path).exists()


class TestCompileYesFlag:
    def test_yes_skips_confirmation(self, tmp_path: Path) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "resources"),
                "--target-dir",
                str(tmp_path / "target"),
                "--yes",
            ],
        )
        assert result.exit_code == 0

    @patch(
        "nagi_cli.commands.compile.list_dbt_origin_dirs",
        return_value=["/some/dbt/dir"],
    )
    def test_declined_confirmation_aborts(
        self, mock_dirs: object, tmp_path: Path
    ) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "resources"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
            input="n\n",
        )
        assert result.exit_code == 0
        assert not (tmp_path / "target" / "graph.json").exists()


class TestCompileFailure:
    def test_missing_resources_dir(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(tmp_path / "nonexistent"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output

    def test_unresolved_upstream_ref(self, tmp_path: Path) -> None:
        resources_dir = tmp_path / "resources"
        resources_dir.mkdir()
        (resources_dir / "asset.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Asset\n"
            "metadata:\n"
            "  name: broken\n"
            "spec:\n"
            "  upstreams:\n"
            "    - nonexistent\n"
        )
        (resources_dir / "sync.yaml").write_text(SYNC_YAML)

        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--resources-dir",
                str(resources_dir),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output


class TestCompilePager:
    def test_success_uses_echo_output(self, tmp_path: Path) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        with patch("nagi_cli.commands.compile.echo_output") as mock_echo:
            runner.invoke(
                compile,
                [
                    "--resources-dir",
                    str(tmp_path / "resources"),
                    "--target-dir",
                    str(tmp_path / "target"),
                ],
            )
            mock_echo.assert_called_once()
            _, kwargs = mock_echo.call_args
            assert kwargs["no_pager"] is False

    def test_no_pager_flag_passed_to_echo_output(self, tmp_path: Path) -> None:
        write_valid_resources(tmp_path / "resources")
        runner = CliRunner()
        with patch("nagi_cli.commands.compile.echo_output") as mock_echo:
            runner.invoke(
                compile,
                [
                    "--resources-dir",
                    str(tmp_path / "resources"),
                    "--target-dir",
                    str(tmp_path / "target"),
                    "--no-pager",
                ],
            )
            mock_echo.assert_called_once()
            _, kwargs = mock_echo.call_args
            assert kwargs["no_pager"] is True
