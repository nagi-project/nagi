import json
from unittest.mock import patch

from click.testing import CliRunner, Result

from nagi_cli.commands.inspect import inspect

SAMPLE_INSPECTION = json.dumps(
    [
        {
            "schema_version": 2,
            "execution_id": "exec-001",
            "asset_name": "daily-sales",
            "finished_at": "2026-04-16T09:30:00.000Z",
            "comparisons": [
                {
                    "type": "condition",
                    "name": "freshness-24h",
                    "before": {"state": "drifted", "reason": "age 30h > max 24h"},
                    "after": {"state": "ready"},
                },
            ],
            "jobs": [],
        }
    ]
)

EMPTY_INSPECTION = json.dumps([])


def _invoke(args: list[str]) -> Result:
    runner = CliRunner()
    return runner.invoke(inspect, args, catch_exceptions=False)


class TestInspectCommand:
    def test_json_output(self) -> None:
        with patch(
            "nagi_cli.commands.inspect.list_inspections",
            return_value=SAMPLE_INSPECTION,
        ):
            result = _invoke(["daily-sales", "--output", "json", "--no-pager"])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["execution_id"] == "exec-001"

    def test_text_output(self) -> None:
        with patch(
            "nagi_cli.commands.inspect.list_inspections",
            return_value=SAMPLE_INSPECTION,
        ):
            result = _invoke(["daily-sales", "--output", "text", "--no-pager"])
        assert result.exit_code == 0, result.output
        assert "daily-sales" in result.output
        assert "exec-001" in result.output

    def test_limit_is_passed(self) -> None:
        with patch(
            "nagi_cli.commands.inspect.list_inspections",
            return_value=SAMPLE_INSPECTION,
        ) as mock:
            result = _invoke(
                ["daily-sales", "--limit", "2", "--output", "json", "--no-pager"]
            )
        assert result.exit_code == 0, result.output
        kwargs = mock.call_args.kwargs
        assert kwargs["limit"] == 2

    def test_changed_only_is_passed(self) -> None:
        with patch(
            "nagi_cli.commands.inspect.list_inspections",
            return_value=SAMPLE_INSPECTION,
        ) as mock:
            result = _invoke(
                [
                    "daily-sales",
                    "--changed-only",
                    "--output",
                    "json",
                    "--no-pager",
                ]
            )
        assert result.exit_code == 0, result.output
        kwargs = mock.call_args.kwargs
        assert kwargs["asset_name"] == "daily-sales"
        assert kwargs["changed_only"] is True

    def test_empty_asset(self) -> None:
        with patch(
            "nagi_cli.commands.inspect.list_inspections",
            return_value=EMPTY_INSPECTION,
        ):
            result = _invoke(["nonexistent", "--output", "text", "--no-pager"])
        assert result.exit_code == 0
        assert "No inspections found" in result.output
