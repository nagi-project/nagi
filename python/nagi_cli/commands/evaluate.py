import json
from pathlib import Path

import click

from nagi_cli._nagi_core import evaluate_asset


@click.command()
@click.argument("yaml_path", type=click.Path(exists=True))
@click.option("--profile", required=True, help="dbt profile name")
@click.option(
    "--target",
    default=None,
    help="dbt target name (uses profile default if omitted)",
)
@click.option(
    "--cache-dir",
    default=None,
    help="Cache directory (defaults to ~/.nagi/cache/)",
)
def evaluate(
    yaml_path: str,
    profile: str,
    target: str | None,
    cache_dir: str | None,
) -> None:
    """Evaluate desired conditions for an asset defined in YAML_PATH."""
    yaml_content = Path(yaml_path).read_text()
    try:
        result_json = evaluate_asset(yaml_content, profile, target, cache_dir)
        click.echo(result_json)
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)
