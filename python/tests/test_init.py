import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from nagi_cli.commands.init import init

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

CONNECTION_OK = json.dumps(
    {
        "status": "ok",
        "adapter": "bigquery",
        "project": "my-gcp-project",
        "dataset": "my_dataset",
    }
)


class TestInitSingleProfile:
    @patch("nagi_cli.commands.init.test_connection", return_value=CONNECTION_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_creates_assets_dir(
        self, _mock_profiles: object, _mock_conn: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(init)
            assert Path("assets").is_dir()

    @patch("nagi_cli.commands.init.test_connection", return_value=CONNECTION_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_creates_config_dir(
        self, _mock_profiles: object, _mock_conn: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            with patch(
                "nagi_cli.commands.init.Path.home",
                return_value=tmp_path,
            ):
                runner.invoke(init)
                assert (tmp_path / ".nagi" / "config.yaml").exists()

    @patch("nagi_cli.commands.init.test_connection", return_value=CONNECTION_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_exit_code_is_zero(
        self, _mock_profiles: object, _mock_conn: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init)
        assert result.exit_code == 0


class TestInitMultipleProfiles:
    @patch("nagi_cli.commands.init.test_connection", return_value=CONNECTION_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=MULTI_PROFILE_JSON,
    )
    def test_prompts_for_profile_selection(
        self, _mock_profiles: object, _mock_conn: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, input="1\ndev\n")
        assert result.exit_code == 0

    @patch("nagi_cli.commands.init.test_connection", return_value=CONNECTION_OK)
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=MULTI_PROFILE_JSON,
    )
    def test_invalid_profile_selection(
        self, _mock_profiles: object, _mock_conn: object, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init, input="99\n")
        assert result.exit_code == 1


class TestInitFailure:
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        side_effect=RuntimeError("profiles.yml not found"),
    )
    def test_profiles_load_error(self, _mock_profiles: object, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init)
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output

    @patch(
        "nagi_cli.commands.init.test_connection",
        side_effect=RuntimeError("connection failed"),
    )
    @patch(
        "nagi_cli.commands.init.load_dbt_profiles",
        return_value=SINGLE_PROFILE_JSON,
    )
    def test_connection_error(
        self,
        _mock_profiles: object,
        _mock_conn: object,
        tmp_path: Path,
    ) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(init)
        assert result.exit_code == 1
        output = json.loads(result.output)
        assert "error" in output
