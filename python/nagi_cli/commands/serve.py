import json

import click

from nagi_cli._nagi_core import serve as _serve
from nagi_cli._nagi_core import serve_resume as _serve_resume


@click.group(invoke_without_command=True)
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
@click.pass_context
def serve(
    ctx: click.Context,
    selectors: tuple[str, ...],
    target_dir: str,
    cache_dir: str | None,
) -> None:
    """Start the reconciliation loop for continuous evaluation."""
    if ctx.invoked_subcommand is not None:
        return
    try:
        _serve(target_dir, list(selectors), cache_dir)
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


@serve.command()
@click.option(
    "--select",
    "selectors",
    multiple=True,
    help="Asset name to resume. Can be repeated.",
)
def resume(selectors: tuple[str, ...]) -> None:
    """Resume suspended assets or list suspended assets."""
    try:
        result_json = _serve_resume(list(selectors))
        names = json.loads(result_json)
        if not selectors:
            if not names:
                click.echo("No suspended assets.")
            else:
                click.echo("Suspended assets:")
                for name in names:
                    click.echo(f"  - {name}")
        else:
            if names:
                for name in names:
                    click.echo(f"Resumed: {name}")
            else:
                click.echo("No matching suspended assets found.")
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)
