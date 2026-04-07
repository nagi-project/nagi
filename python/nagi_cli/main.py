import click

from nagi_cli._nagi_core import init_log, set_log_level
from nagi_cli.commands.compile import compile
from nagi_cli.commands.evaluate import evaluate
from nagi_cli.commands.export import export
from nagi_cli.commands.init import init
from nagi_cli.commands.ls import ls
from nagi_cli.commands.mcp import mcp
from nagi_cli.commands.serve import serve
from nagi_cli.commands.status import status
from nagi_cli.commands.sync import sync

LOG_LEVELS = ("error", "warn", "info", "debug", "trace")


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default=None,
    help="Set log level (default: warn, or NAGI_LOG_LEVEL env var).",
)
def cli(log_level: str | None) -> None:
    if log_level is not None:
        set_log_level(log_level)
    else:
        init_log()


cli.add_command(init)
cli.add_command(compile)
cli.add_command(evaluate)
cli.add_command(export)
cli.add_command(ls)
cli.add_command(mcp)
cli.add_command(sync)
cli.add_command(serve)
cli.add_command(status)


if __name__ == "__main__":
    cli()
