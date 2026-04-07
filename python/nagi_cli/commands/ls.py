import json

import click

from nagi_cli._nagi_core import format_ls_text, list_resources
from nagi_cli.output import OUTPUT_FORMATS, echo_output


@click.command("ls")
@click.option(
    "--target-dir",
    default="target",
    show_default=True,
    help="Directory containing compiled output.",
)
@click.option(
    "--kind",
    "kinds",
    multiple=True,
    help="Filter by resource kind (e.g. Asset, Connection).",
)
@click.option(
    "--output",
    "output_format",
    type=click.Choice(OUTPUT_FORMATS, case_sensitive=False),
    default="json",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--no-pager",
    is_flag=True,
    default=False,
    help="Disable pager for terminal output.",
)
def ls(
    target_dir: str,
    kinds: tuple[str, ...],
    output_format: str,
    no_pager: bool,
) -> None:
    """List all compiled resources."""
    try:
        result_json = list_resources(target_dir, list(kinds))
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    output = format_ls_text(result_json) if output_format == "text" else result_json
    echo_output(output, no_pager=no_pager)
