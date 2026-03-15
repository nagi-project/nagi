import json

import click

from nagi_cli._nagi_core import compile_assets, list_dbt_origins


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
        if not yes and _should_skip_dbt_compile(assets_dir):
            return

        graph_json = compile_assets(assets_dir, target_dir)
        graph = json.loads(graph_json)
        click.echo(
            json.dumps(
                {
                    "nodes": len(graph["nodes"]),
                    "edges": len(graph["edges"]),
                    "target": target_dir,
                }
            )
        )
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


def _should_skip_dbt_compile(assets_dir: str) -> bool:
    origins_json = list_dbt_origins(assets_dir)
    origins = json.loads(origins_json)
    if not origins:
        return False
    dirs = ", ".join(o["projectDir"] for o in origins)
    return not click.confirm(
        f"This will run `dbt compile` for: {dirs}. Continue?",
        default=True,
    )
