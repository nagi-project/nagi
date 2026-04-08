import json

import click

from nagi_cli._nagi_core import compile_assets, list_dbt_origin_dirs
from nagi_cli.output import echo_output


@click.command()
@click.option(
    "--resources-dir",
    default="resources",
    show_default=True,
    help="Directory containing resource YAML files.",
)
@click.option(
    "--target-dir",
    default="target",
    show_default=True,
    help="Directory to write compiled output.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompts.",
)
@click.option(
    "--no-pager",
    is_flag=True,
    default=False,
    help="Disable pager for terminal output.",
)
def compile(resources_dir: str, target_dir: str, yes: bool, no_pager: bool) -> None:
    """Compile resource definitions into resolved target output."""
    try:
        if not yes:
            dirs = list_dbt_origin_dirs(resources_dir)
            if dirs and not click.confirm(
                f"This will run `dbt compile` for: {dirs}. Continue?",
                default=True,
            ):
                return

        output = compile_assets(resources_dir, target_dir, project_dir=".")
        echo_output(output, no_pager=no_pager)
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}), err=True)
        raise SystemExit(1)
