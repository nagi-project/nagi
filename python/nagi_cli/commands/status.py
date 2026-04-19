import json

import click

from nagi_cli._nagi_core import asset_status, format_status_text
from nagi_cli.output import OUTPUT_FORMATS, echo_output


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
    "--output",
    "output_format",
    type=click.Choice(OUTPUT_FORMATS, case_sensitive=False),
    default="json",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--no-pager",
    is_flag=True,
    default=False,
    help="Disable pager for terminal output.",
)
def status(
    selectors: tuple[str, ...],
    excludes: tuple[str, ...],
    target_dir: str,
    output_format: str,
    no_pager: bool,
) -> None:
    """Show current convergence status (reads cache and latest sync log)."""
    try:
        result_json = asset_status(
            target_dir=target_dir,
            selectors=list(selectors),
            excludes=list(excludes),
        )
    except (RuntimeError, json.JSONDecodeError) as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    output = format_status_text(result_json) if output_format == "text" else result_json
    echo_output(output, no_pager=no_pager)
