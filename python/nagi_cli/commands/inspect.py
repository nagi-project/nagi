import click

from nagi_cli._nagi_core import format_inspect_text, list_inspections
from nagi_cli.output import OUTPUT_FORMATS, echo_output


@click.command()
@click.argument("asset_name")
@click.option(
    "--limit",
    default=5,
    show_default=True,
    help="Maximum number of recent sync executions to show.",
)
@click.option(
    "--target-dir",
    default="target",
    show_default=True,
    help="Directory containing compiled output.",
)
@click.option(
    "--changed-only",
    is_flag=True,
    default=False,
    help="Show only executions where state changed between before and after Sync.",
)
@click.option(
    "--output",
    "output_format",
    type=click.Choice(OUTPUT_FORMATS, case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--no-pager",
    is_flag=True,
    default=False,
    help="Disable pager for terminal output.",
)
@click.option(
    "--nagi-dir",
    default=None,
    help="Override Nagi state directory path.",
)
def inspect(
    asset_name: str,
    limit: int,
    target_dir: str,
    changed_only: bool,
    output_format: str,
    no_pager: bool,
    nagi_dir: str | None,
) -> None:
    """Show sync execution inspection records for an asset."""
    json_str = list_inspections(asset_name, limit, target_dir, changed_only, nagi_dir)
    output = format_inspect_text(json_str) if output_format == "text" else json_str
    echo_output(output, no_pager=no_pager)
