import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nagi_cli.commands.init import _read_dbt_origin, init

SINGLE_PROFILE_JSON = json.dumps(
    {
        "profiles": [
            {
                "name": "my_project",
                "defaultTarget": "dev",
                "targets": ["dev"],
            }
        ]
    }
)

MULTI_PROFILE_JSON = json.dumps(
    {
        "profiles": [
            {
                "name": "project_a",
                "defaultTarget": "dev",
                "targets": ["dev", "prod"],
            },
            {
                "name": "project_b",
                "defaultTarget": "staging",
                "targets": ["staging"],
            },
        ]
    }
)

SINGLE_MULTI_TARGET_JSON = json.dumps(
    {
        "profiles": [
            {
                "name": "my_project",
                "defaultTarget": "dev",
                "targets": ["dev", "prod"],
            }
        ]
    }
)

DBT_DEBUG_OK = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")


class TestInitNoDbt:
    def test_creates_assets_dir_without_dbt(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, input="n\n")
            assert result.exit_code == 0
            assert Path("assets").is_dir()

    def test_creates_config_without_dbt(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            with patch(
                "nagi_cli.commands.init.Path.home",
                return_value=tmp_path,
            ):
                result = runner.invoke(init, input="n\n")
                assert result.exit_code == 0
                assert (tmp_path / ".nagi" / "config.yaml").exists()

    def test_no_connection_or_origin(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, input="n\n")
            assert result.exit_code == 0
            assert not (Path("assets") / "connection.yaml").exists()
            assert not (Path("assets") / "origin.yaml").exists()


class TestInitSingleProfile:
    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_generates_connection_yaml(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir = tmp_path / "dbt-project"
        dbt_dir.mkdir()
        (dbt_dir / "dbt_project.yml").write_text("name: test_project\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir}\nn\n")
            assert result.exit_code == 0
            connection_path = Path("assets") / "connection.yaml"
            assert connection_path.exists()
            content = connection_path.read_text()
            assert "kind: Connection" in content
            assert "profile: my_project" in content

    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_generates_origin_yaml_with_dbt_project(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir = tmp_path / "dbt-project"
        dbt_dir.mkdir()
        (dbt_dir / "dbt_project.yml").write_text("name: jaffle_shop\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir}\nn\n")
            assert result.exit_code == 0
            assert (Path("assets") / "origin.yaml").exists()

    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_generates_multiple_origins(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir_a = tmp_path / "dbt-a"
        dbt_dir_a.mkdir()
        (dbt_dir_a / "dbt_project.yml").write_text("name: project_a\n")
        dbt_dir_b = tmp_path / "dbt-b"
        dbt_dir_b.mkdir()
        (dbt_dir_b / "dbt_project.yml").write_text("name: project_b\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir_a}\ny\n{dbt_dir_b}\nn\n")
            assert result.exit_code == 0
            content = (Path("assets") / "origin.yaml").read_text()
            assert content.count("kind: Origin") == 2


class TestInitMultipleProfiles:
    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=MULTI_PROFILE_JSON,
    )
    def test_prompts_for_profile_selection(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir = tmp_path / "dbt-project"
        dbt_dir.mkdir()
        (dbt_dir / "dbt_project.yml").write_text("name: test\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir}\n1\ndev\nn\n")
        assert result.exit_code == 0

    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=MULTI_PROFILE_JSON,
    )
    def test_invalid_profile_selection(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir = tmp_path / "dbt-project"
        dbt_dir.mkdir()
        (dbt_dir / "dbt_project.yml").write_text("name: test\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir}\n99\n")
        assert result.exit_code == 1

    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=MULTI_PROFILE_JSON,
    )
    def test_per_origin_connection(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir_a = tmp_path / "dbt-a"
        dbt_dir_a.mkdir()
        (dbt_dir_a / "dbt_project.yml").write_text("name: proj_a\n")
        dbt_dir_b = tmp_path / "dbt-b"
        dbt_dir_b.mkdir()
        (dbt_dir_b / "dbt_project.yml").write_text("name: proj_b\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            # First project uses profile 1 (project_a/dev)
            # Second project uses profile 2 (project_b/staging)
            result = runner.invoke(
                init,
                input=f"y\n{dbt_dir_a}\n1\ndev\ny\n{dbt_dir_b}\n2\nn\n",
            )
            assert result.exit_code == 0

            conn_content = (Path("assets") / "connection.yaml").read_text()
            assert "name: project_a-dev" in conn_content
            assert "name: project_b-staging" in conn_content
            assert conn_content.count("kind: Connection") == 2

            origin_content = (Path("assets") / "origin.yaml").read_text()
            assert "connection: project_a-dev" in origin_content
            assert "connection: project_b-staging" in origin_content
            assert origin_content.count("kind: Origin") == 2

    @patch("nagi_cli.commands.init.subprocess.run", return_value=DBT_DEBUG_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_MULTI_TARGET_JSON,
    )
    def test_same_profile_different_targets(
        self, _mock_profiles: object, _mock_run: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        dbt_dir_a = tmp_path / "dbt-a"
        dbt_dir_a.mkdir()
        (dbt_dir_a / "dbt_project.yml").write_text("name: proj_a\n")
        dbt_dir_b = tmp_path / "dbt-b"
        dbt_dir_b.mkdir()
        (dbt_dir_b / "dbt_project.yml").write_text("name: proj_b\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(
                init,
                input=f"y\n{dbt_dir_a}\ndev\ny\n{dbt_dir_b}\nprod\nn\n",
            )
            assert result.exit_code == 0

            conn_content = (Path("assets") / "connection.yaml").read_text()
            assert conn_content.count("kind: Connection") == 2
            assert "name: my_project-dev" in conn_content
            assert "name: my_project-prod" in conn_content

            origin_content = (Path("assets") / "origin.yaml").read_text()
            assert "connection: my_project-dev" in origin_content
            assert "connection: my_project-prod" in origin_content


class TestInitFailure:
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        side_effect=RuntimeError("profiles.yml not found"),
    )
    def test_profiles_load_error(self, _mock_profiles: object, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, input="y\n")
        assert result.exit_code == 1
        assert "profiles.yml not found" in result.output

    @patch(
        "nagi_cli.commands.init.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=1, stdout="Connection test failed", stderr=""
        ),
    )
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_connection_error(
        self,
        _mock_profiles: object,
        _mock_run: object,
        tmp_path: Path,
    ) -> None:
        runner = CliRunner()
        dbt_dir = tmp_path / "dbt-project"
        dbt_dir.mkdir()
        (dbt_dir / "dbt_project.yml").write_text("name: test\n")
        nagi_dir = tmp_path / "nagi"
        nagi_dir.mkdir()
        with runner.isolated_filesystem(temp_dir=nagi_dir):
            result = runner.invoke(init, input=f"y\n{dbt_dir}\n")
        assert result.exit_code == 1
        assert "dbt debug failed" in result.output


class TestReadDbtOrigin:
    def test_returns_origin_yaml(self, tmp_path: Path) -> None:
        (tmp_path / "dbt_project.yml").write_text("name: jaffle_shop\n")
        result = _read_dbt_origin(tmp_path, "my-bq")
        assert "kind: Origin" in result
        assert "name: jaffle_shop" in result
        assert "connection: my-bq" in result
        assert f"projectDir: {tmp_path}" in result

    def test_uses_default_name_when_missing(self, tmp_path: Path) -> None:
        (tmp_path / "dbt_project.yml").write_text("{}\n")
        result = _read_dbt_origin(tmp_path, "my-bq")
        assert "name: my-dbt-project" in result

    def test_raises_when_no_dbt_project_yml(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit):
            _read_dbt_origin(tmp_path, "my-bq")
