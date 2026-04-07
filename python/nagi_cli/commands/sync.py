import json

import click

from nagi_cli._nagi_core import (
    execute_sync_proposal,
    propose_sync,
    try_export,
)


def _make_sync_command(sync_type: str) -> click.Command:
    @click.command(
        name=sync_type,
        help="Execute sync convergence operation for assets.",
    )
    @click.option(
        "--select",
        "selectors",
        multiple=True,
        help="Asset selector expression (dbt-compatible). Can be repeated.",
    )
    @click.option(
        "--exclude",
        "excludes",
        multiple=True,
        help="Exclude assets matching this selector. Can be repeated.",
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
        help="Cache directory (defaults to &lt;nagiDir&gt;/cache/)",
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
    @click.option(
        "--auto-approve",
        is_flag=True,
        default=False,
        help="Skip interactive confirmation and execute all proposals.",
    )
    def cmd(
        selectors: tuple[str, ...],
        excludes: tuple[str, ...],
        target_dir: str,
        stages: str | None,
        cache_dir: str | None,
        dry_run: bool,
        force: bool,
        auto_approve: bool,
    ) -> None:
        try:
            proposals = json.loads(
                propose_sync(
                    target_dir=target_dir,
                    selectors=list(selectors),
                    sync_type=sync_type,
                    excludes=list(excludes),
                    stages=stages,
                    cache_dir=cache_dir,
                )
            )
        except (RuntimeError, json.JSONDecodeError) as e:
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
            if not auto_approve and not click.confirm("Run sync?", default=True):
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

        if not dry_run:
            try_export()

    return cmd


sync = _make_sync_command("sync")
