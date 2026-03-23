import click

from nagi_cli._nagi_core import evaluate_all, try_export


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
    help="Cache directory (defaults to &lt;nagiDir&gt;/cache/)",
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
    try:
        result_json = evaluate_all(target_dir, list(selectors), cache_dir, dry_run)
    except RuntimeError as e:
        import json

        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    click.echo(result_json)

    if not dry_run:
        try_export()
