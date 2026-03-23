import click

from nagi_cli.commands.compile import compile
from nagi_cli.commands.evaluate import evaluate
from nagi_cli.commands.export import export
from nagi_cli.commands.init import init
from nagi_cli.commands.ls import ls
from nagi_cli.commands.mcp import mcp
from nagi_cli.commands.serve import serve
from nagi_cli.commands.status import status
from nagi_cli.commands.sync import sync


@click.group()
def cli() -> None:
    pass


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
