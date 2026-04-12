import json
from unittest.mock import patch

from click.testing import CliRunner

from nagi_cli.commands.export import export

MOCK_DRY_RUN = json.dumps(
    [{"table": "evaluate_logs", "rows": 10}, {"table": "sync_logs", "rows": 5}]
)
MOCK_EXPORT = json.dumps([{"table": "evaluate_logs", "rows_exported": 10}])


class TestExportDryRun:
    def test_dry_run_calls_export_dry_run(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_dry_run",
            return_value=MOCK_DRY_RUN,
        ) as mock:
            result = runner.invoke(export, ["--dry-run"])
        assert result.exit_code == 0
        mock.assert_called_once_with(select=None)

    def test_dry_run_with_select_passes_table_name(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_dry_run",
            return_value=MOCK_DRY_RUN,
        ) as mock:
            result = runner.invoke(export, ["--dry-run", "--select", "evaluate_logs"])
        assert result.exit_code == 0
        mock.assert_called_once_with(select="evaluate_logs")

    def test_dry_run_outputs_json(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_dry_run",
            return_value=MOCK_DRY_RUN,
        ):
            result = runner.invoke(export, ["--dry-run"])
        output = json.loads(result.output.strip())
        assert len(output) == 2
        assert output[0]["table"] == "evaluate_logs"


class TestExportFull:
    def test_export_calls_export_logs(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_logs",
            return_value=MOCK_EXPORT,
        ) as mock:
            result = runner.invoke(export, [])
        assert result.exit_code == 0
        mock.assert_called_once_with(select=None)

    def test_export_with_select(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_logs",
            return_value=MOCK_EXPORT,
        ) as mock:
            result = runner.invoke(export, ["--select", "sync_logs"])
        assert result.exit_code == 0
        mock.assert_called_once_with(select="sync_logs")

    def test_export_runtime_error_prints_warning(self) -> None:
        runner = CliRunner()
        with patch(
            "nagi_cli.commands.export.export_logs",
            side_effect=RuntimeError("connection failed"),
        ):
            result = runner.invoke(export, [])
        assert result.exit_code == 0
        assert "connection failed" in result.output
