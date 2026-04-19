import click

from nagi_cli._nagi_core import evaluate_all, format_evaluate_text, try_export
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
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show which assets would be evaluated without executing.",
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
def evaluate(
    selectors: tuple[str, ...],
    excludes: tuple[str, ...],
    target_dir: str,
    dry_run: bool,
    output_format: str,
    no_pager: bool,
) -> None:
    """Evaluate desired conditions for assets from compiled target output."""
    try:
        result_json = evaluate_all(
            target_dir=target_dir,
            selectors=list(selectors),
            excludes=list(excludes),
            dry_run=dry_run,
        )
    except RuntimeError as e:
        import json

        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if output_format == "text":
        output = format_evaluate_text(result_json)
    else:
        output = result_json
    echo_output(output, no_pager=no_pager)

    if not dry_run:
        try_export()
