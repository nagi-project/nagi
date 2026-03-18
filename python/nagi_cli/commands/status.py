import click

from nagi_cli._nagi_core import asset_status


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
def status(
    selectors: tuple[str, ...],
    target_dir: str,
    cache_dir: str | None,
) -> None:
    """Show current convergence status (reads cache and latest sync log)."""
    try:
        result_json = asset_status(target_dir, list(selectors), cache_dir)
    except RuntimeError as e:
        import json

        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    click.echo(result_json)
