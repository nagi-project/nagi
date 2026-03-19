import json

import click

from nagi_cli._nagi_core import (
    init_workspace,
    load_dbt_profiles,
    run_dbt_debug,
    write_init_dbt_files,
)


@click.command()
def init() -> None:
    """Prepare the environment so that `nagi compile` can run."""
    try:
        init_workspace()
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if not click.confirm("Do you use dbt?", default=False):
        return

    try:
        profiles = json.loads(load_dbt_profiles())["profiles"]
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    entries = _collect_dbt_entries(profiles)
    if not entries:
        return

    try:
        result = json.loads(write_init_dbt_files(".", json.dumps(entries)))
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if result.get("connectionPath"):
        click.echo(json.dumps({"connection": result["connectionPath"]}))
    if result.get("originPath"):
        click.echo(json.dumps({"origin": result["originPath"]}))


def _collect_dbt_entries(profiles: list[dict]) -> list[dict]:
    """Interactively collect dbt project entries from the user."""
    entries: list[dict] = []
    tested: set[tuple[str, str | None]] = set()

    while True:
        dbt_dir = click.prompt("Path to dbt project directory")
        profile_name, target = _select_profile_target(profiles)

        if (profile_name, target) not in tested:
            try:
                run_dbt_debug(dbt_dir, profile_name, target)
            except RuntimeError as e:
                click.echo(json.dumps({"error": str(e)}))
                raise SystemExit(1)
            click.echo(json.dumps({"connection": "ok"}))
            tested.add((profile_name, target))

        entries.append(
            {
                "projectDir": dbt_dir,
                "profile": profile_name,
                "target": target,
            }
        )

        if not click.confirm("Do you have another dbt project?", default=False):
            break

    return entries


def _select_profile_target(
    profiles: list[dict],
) -> tuple[str, str | None]:
    if not profiles:
        click.echo(json.dumps({"error": "no profiles found in ~/.dbt/profiles.yml"}))
        raise SystemExit(1)

    if len(profiles) == 1:
        profile = profiles[0]
    else:
        click.echo("Available profiles:")
        for i, p in enumerate(profiles):
            click.echo(f"  {i + 1}) {p['name']} (targets: {', '.join(p['targets'])})")
        choice = click.prompt("Select profile", type=int) - 1
        if choice < 0 or choice >= len(profiles):
            click.echo(json.dumps({"error": "invalid selection"}))
            raise SystemExit(1)
        profile = profiles[choice]

    profile_name = profile["name"]

    targets = profile["targets"]
    if len(targets) == 1:
        target = targets[0]
    else:
        default_target = profile["defaultTarget"]
        click.echo(f"Available targets for '{profile_name}': {', '.join(targets)}")
        target = click.prompt("Select target", default=default_target)
        if target not in targets:
            click.echo(json.dumps({"error": f"target '{target}' not found"}))
            raise SystemExit(1)

    return profile_name, target
