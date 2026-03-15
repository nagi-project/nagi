import json

import click

from nagi_cli._nagi_core import compile_assets


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
def compile(assets_dir: str, target_dir: str) -> None:
    """Compile asset definitions into resolved target output."""
    try:
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
