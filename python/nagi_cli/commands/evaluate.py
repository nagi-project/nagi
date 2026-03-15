import json
from pathlib import Path

import click

from nagi_cli._nagi_core import dry_run_asset, evaluate_asset, select_assets


@click.command()
@click.option(
    "--select",
    "selectors",
    multiple=True,
    help="Asset selector expression (dbt-compatible). Can be repeated.",
)
@click.option(
    "--target-dir",
    default="target",
    show_default=True,
    help="Directory containing compiled output.",
)
@click.option(
    "--cache-dir",
    default=None,
    help="Cache directory (defaults to ~/.nagi/cache/)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show which assets would be evaluated without executing.",
)
def evaluate(
    selectors: tuple[str, ...],
    target_dir: str,
    cache_dir: str | None,
    dry_run: bool,
) -> None:
    """Evaluate desired conditions for assets from compiled target output."""
    target_path = Path(target_dir)
    assets_path = target_path / "assets"
    graph_path = target_path / "graph.json"

    if not graph_path.exists():
        click.echo(
            json.dumps({"error": f"{graph_path} not found. Run 'nagi compile' first."})
        )
        raise SystemExit(1)

    asset_names = _resolve_asset_names(graph_path, selectors, assets_path)

    if dry_run:
        results = []
        for name in asset_names:
            yaml_file = assets_path / f"{name}.yaml"
            if not yaml_file.exists():
                msg = f"compiled asset not found: {yaml_file}"
                click.echo(json.dumps({"error": msg}))
                raise SystemExit(1)
            result_json = dry_run_asset(yaml_file.read_text())
            results.append(json.loads(result_json))
        click.echo(json.dumps({"dry_run": True, "assets": results}))
        return

    results = []
    for name in asset_names:
        yaml_file = assets_path / f"{name}.yaml"
        if not yaml_file.exists():
            msg = f"compiled asset not found: {yaml_file}"
            click.echo(json.dumps({"error": msg}))
            raise SystemExit(1)
        yaml_content = yaml_file.read_text()
        try:
            result_json = evaluate_asset(yaml_content, cache_dir)
            result = json.loads(result_json)
            results.append(result)
            click.echo(json.dumps(result))
        except RuntimeError as e:
            click.echo(json.dumps({"error": str(e), "asset": name}))
            raise SystemExit(1)


def _resolve_asset_names(
    graph_path: Path,
    selectors: tuple[str, ...],
    assets_path: Path,
) -> list[str]:
    graph_json = graph_path.read_text()

    if selectors:
        selected_json = select_assets(graph_json, list(selectors))
        return json.loads(selected_json)

    # No selectors: evaluate all assets in target/assets/
    return sorted(p.stem for p in assets_path.glob("*.yaml"))
