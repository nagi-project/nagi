import click


@click.command()
@click.option(
    "--allow-sync",
    is_flag=True,
    default=False,
    help="Also expose nagi_sync and nagi_resync tools.",
)
def mcp(allow_sync: bool) -> None:
    """Start MCP server on stdio (for AI agent integration)."""
    from nagi_cli.mcp import run_stdio

    run_stdio(allow_sync=allow_sync)
