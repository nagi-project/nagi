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
    """Resume suspended assets or list suspended assets.

    With --select: resume the specified assets.
    Without --select: show suspended assets and interactively select which to resume.
    """
    try:
        if selectors:
            result_json = _serve_resume(list(selectors))
            names = json.loads(result_json)
            if names:
                for name in names:
                    click.echo(f"Resumed: {name}")
            else:
                click.echo("No matching suspended assets found.")
            return

        # No selectors: list and interactively select.
        result_json = _serve_resume([])
        names: list[str] = json.loads(result_json)
        if not names:
            click.echo("No suspended assets.")
            return

        click.echo("Suspended assets:")
        for i, name in enumerate(names, 1):
            click.echo(f"  {i}. {name}")

        selection = click.prompt(
            "Enter numbers to resume (comma-separated), or 'all'",
            default="",
            show_default=False,
        )
        selection = selection.strip()
        if not selection:
            click.echo("No assets selected.")
            return

        if selection.lower() == "all":
            to_resume = names
        else:
            indices = []
            for part in selection.split(","):
                part = part.strip()
                if not part.isdigit():
                    click.echo(f"Invalid input: {part}")
                    return
                idx = int(part)
                if idx < 1 or idx > len(names):
                    click.echo(f"Out of range: {idx}")
                    return
                indices.append(idx)
            to_resume = [names[i - 1] for i in indices]

        result_json = _serve_resume(to_resume)
        resumed = json.loads(result_json)
        for name in resumed:
            click.echo(f"Resumed: {name}")
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)
