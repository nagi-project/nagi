import json

import click

from nagi_cli._nagi_core import (
    execute_sync_proposal,
    propose_sync,
)


def _make_sync_command(sync_type: str) -> click.Command:
    help_text = (
        "Execute sync convergence operation for assets."
        if sync_type == "sync"
        else "Execute resync (radical repair) convergence operation for assets."
    )

    @click.command(name=sync_type, help=help_text)
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
        "--stage",
        "stages",
        default=None,
        help="Comma-separated stages to execute (e.g. pre,run).",
    )
    @click.option(
        "--cache-dir",
        default=None,
        help="Cache directory (defaults to ~/.nagi/cache/)",
    )
    @click.option(
        "--dry-run",
        is_flag=True,
        default=False,
        help="Show commands that would be executed without running them.",
    )
    @click.option(
        "--force",
        is_flag=True,
        default=False,
        help="Skip pre-flight checks (e.g. dbt Cloud running jobs).",
    )
    def cmd(
        selectors: tuple[str, ...],
        target_dir: str,
        stages: str | None,
        cache_dir: str | None,
        dry_run: bool,
        force: bool,
    ) -> None:
        try:
            proposals = json.loads(
                propose_sync(target_dir, list(selectors), sync_type, stages, cache_dir)
            )
        except RuntimeError as e:
            click.echo(json.dumps({"error": str(e)}))
            raise SystemExit(1)

        for proposal in proposals:
            if dry_run:
                click.echo(
                    json.dumps(
                        {
                            "asset": proposal["asset"],
                            "syncType": proposal["syncType"],
                            "stages": proposal.get("stages", []),
                        }
                    )
                )
                continue

            click.echo(json.dumps({"proposal": proposal}))
            if not click.confirm("Run sync?", default=True):
                click.echo(json.dumps({"skipped": proposal["asset"]}))
                continue

            try:
                result_json = execute_sync_proposal(
                    json.dumps(proposal), sync_type, stages, cache_dir, force
                )
            except RuntimeError as e:
                click.echo(json.dumps({"error": str(e), "asset": proposal["asset"]}))
                raise SystemExit(1)

            click.echo(result_json)

    return cmd


sync = _make_sync_command("sync")
resync = _make_sync_command("resync")
