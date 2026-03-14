import click

from nagi_cli.commands.evaluate import evaluate
from nagi_cli.commands.init import init


@click.group()
def cli() -> None:
    pass


cli.add_command(init)
cli.add_command(evaluate)


if __name__ == "__main__":
    cli()
