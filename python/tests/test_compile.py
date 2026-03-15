import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nagi_cli.commands.compile import compile
from tests.helper import ASSET_NAME, SYNC_YAML, write_valid_assets


class TestCompileSuccess:
    def test_exit_code_is_zero(self, tmp_path: Path) -> None:
        write_valid_assets(tmp_path / "assets")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--assets-dir",
                str(tmp_path / "assets"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert result.exit_code == 0

    def test_output_contains_graph_summary(self, tmp_path: Path) -> None:
        write_valid_assets(tmp_path / "assets")
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--assets-dir",
                str(tmp_path / "assets"),
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
        write_valid_assets(tmp_path / "assets")
        runner = CliRunner()
        runner.invoke(
            compile,
            [
                "--assets-dir",
                str(tmp_path / "assets"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert (tmp_path / "target" / expected_path).exists()


class TestCompileFailure:
    def test_missing_assets_dir(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            compile,
            [
                "--assets-dir",
                str(tmp_path / "nonexistent"),
                "--target-dir",
                str(tmp_path / "target"),
            ],
        )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output

    def test_unresolved_source_ref(self, tmp_path: Path) -> None:
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()
        (assets_dir / "asset.yaml").write_text(
            "kind: Asset\n"
            "metadata:\n"
            "  name: broken\n"
            "spec:\n"
            "  sources:\n"
            "    - ref: nonexistent\n"
            "  sync:\n"
            "    ref: dbt-sync\n"
        )
        (assets_dir / "sync.yaml").write_text(SYNC_YAML)

        runner = CliRunner()
        result = runner.invoke(
            compile,
            ["--assets-dir", str(assets_dir), "--target-dir", str(tmp_path / "target")],
        )
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output
