import json

import click

from nagi_cli._nagi_core import format_ls_text, list_resources

OUTPUT_FORMATS = ("json", "text")


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
def ls(target_dir: str, kinds: tuple[str, ...], output_format: str) -> None:
    """List all compiled resources."""
    try:
        result_json = list_resources(target_dir, list(kinds))
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if output_format == "text":
        click.echo(format_ls_text(result_json))
    else:
        click.echo(result_json)
