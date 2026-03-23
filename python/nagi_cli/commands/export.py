import click

from nagi_cli._nagi_core import export_dry_run, export_logs


@click.command()
@click.option("--select", "select_table", default=None, help="Table name to export")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show unexported row counts without transferring",
)
def export(select_table: str | None, dry_run: bool) -> None:
    """Export logs to remote DWH."""
    if dry_run:
        result_json = export_dry_run(select=select_table)
        click.echo(result_json)
        return

    try:
        result_json = export_logs(select=select_table)
        click.echo(result_json)
    except RuntimeError as e:
        click.echo(f"[export] warning: {e}", err=True)
