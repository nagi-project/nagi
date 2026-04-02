import click


@click.command()
@click.option(
    "--allow-sync",
    is_flag=True,
    default=False,
    help="Also expose nagi_sync tool.",
)
def mcp(allow_sync: bool) -> None:
    """Start MCP server on stdio (for AI agent integration)."""
    try:
        from nagi_cli.mcp import run_stdio
    except ImportError:
        raise click.ClickException(
            "The 'mcp' extra is required for this command. "
            "Install it with: pip install nagi-cli[mcp]"
        )

    run_stdio(allow_sync=allow_sync)
