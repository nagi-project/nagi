import json

import click

from nagi_cli._nagi_core import compile_assets, list_dbt_origin_dirs


@click.command()
@click.option(
    "--assets-dir",
    default="assets",
    show_default=True,
    help="Directory containing asset YAML files.",
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
def compile(assets_dir: str, target_dir: str, yes: bool) -> None:
    """Compile asset definitions into resolved target output."""
    try:
        if not yes:
            dirs = list_dbt_origin_dirs(assets_dir)
            if dirs and not click.confirm(
                f"This will run `dbt compile` for: {dirs}. Continue?",
                default=True,
            ):
                return

        click.echo(compile_assets(assets_dir, target_dir))
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)
