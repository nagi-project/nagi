import click

from nagi_cli._nagi_core import format_inspect_text, list_inspections
from nagi_cli.output import OUTPUT_FORMATS, echo_output


@click.command()
@click.argument("asset_name")
@click.option(
    "--last",
    default=5,
    show_default=True,
    help="Number of most recent sync executions to show.",
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
def inspect(
    asset_name: str,
    last: int,
    output_format: str,
    no_pager: bool,
) -> None:
    """Show sync execution inspection records for an asset."""
    json_str = list_inspections(asset_name, last)
    output = format_inspect_text(json_str) if output_format == "text" else json_str
    echo_output(output, no_pager=no_pager)
