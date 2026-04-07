import json

import click

from nagi_cli._nagi_core import asset_status, format_status_text

OUTPUT_FORMATS = ("json", "text")


@click.command()
@click.option(
    "--select",
    "selectors",
    multiple=True,
    help="Asset selector expression (dbt-compatible). Can be repeated.",
)
@click.option(
    "--exclude",
    "excludes",
    multiple=True,
    help="Exclude assets matching this selector. Can be repeated.",
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
    help="Cache directory (defaults to <nagiDir>/cache/)",
)
@click.option(
    "--output",
    "output_format",
    type=click.Choice(OUTPUT_FORMATS, case_sensitive=False),
    default="json",
    show_default=True,
    help="Output format.",
)
def status(
    selectors: tuple[str, ...],
    excludes: tuple[str, ...],
    target_dir: str,
    cache_dir: str | None,
    output_format: str,
) -> None:
    """Show current convergence status (reads cache and latest sync log)."""
    try:
        result_json = asset_status(
            target_dir=target_dir,
            selectors=list(selectors),
            excludes=list(excludes),
            cache_dir=cache_dir,
        )
    except (RuntimeError, json.JSONDecodeError) as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if output_format == "text":
        click.echo(format_status_text(result_json))
    else:
        click.echo(result_json)
