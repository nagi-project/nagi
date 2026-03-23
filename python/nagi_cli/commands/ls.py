import json

import click

from nagi_cli._nagi_core import list_resources


@click.command("ls")
@click.option(
    "--target-dir",
    default="target",
    show_default=True,
    help="Directory containing compiled output.",
)
def ls(target_dir: str) -> None:
    """List all compiled resources as JSON."""
    try:
        result_json = list_resources(target_dir)
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    click.echo(result_json)
