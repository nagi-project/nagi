import sys

import click

OUTPUT_FORMATS = ("json", "text")


def echo_output(text: str, *, no_pager: bool) -> None:
    """Outputs text, using a pager when stdout is a TTY and --no-pager is not set."""
    if not no_pager and sys.stdout.isatty():
        click.echo_via_pager(text)
    else:
        click.echo(text)
