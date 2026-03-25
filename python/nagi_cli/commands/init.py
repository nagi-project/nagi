import json

import click

from nagi_cli._nagi_core import (
    init_workspace,
    load_dbt_profiles,
    run_dbt_debug,
    write_init_dbt_files,
)

DOCS_URL = "https://nagi-project.dev"

ORIGIN_TYPES = [
    {"key": "dbt", "label": "dbt project"},
]


@click.command()
def init() -> None:
    """Prepare the environment so that `nagi compile` can run."""
    try:
        init_workspace()
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if not click.confirm("Set up an Origin?", default=True):
        click.echo(f"See {DOCS_URL} for manual configuration.")
        return

    dbt_entries: list[dict] = []
    dbt_tested: set[tuple[str, str | None]] = set()
    dbt_profiles: list[dict] | None = None

    while True:
        origin_type = _select_origin_type()

        if origin_type == "dbt":
            if dbt_profiles is None:
                try:
                    dbt_profiles = json.loads(load_dbt_profiles())["profiles"]
                except RuntimeError as e:
                    click.echo(json.dumps({"error": str(e)}))
                    raise SystemExit(1)
            _collect_one_dbt_entry(dbt_entries, dbt_tested, dbt_profiles)

        if not click.confirm("Add another Origin?", default=False):
            break

    if dbt_entries:
        try:
            result = json.loads(write_init_dbt_files(".", json.dumps(dbt_entries)))
        except RuntimeError as e:
            click.echo(json.dumps({"error": str(e)}))
            raise SystemExit(1)

        if result.get("connectionPath"):
            click.echo(json.dumps({"connection": result["connectionPath"]}))
        if result.get("originPath"):
            click.echo(json.dumps({"origin": result["originPath"]}))


def _select_origin_type() -> str:
    click.echo("Origin types:")
    for i, entry in enumerate(ORIGIN_TYPES):
        click.echo(f"  {i + 1}) {entry['label']}")
    choice = click.prompt("Select Origin type", type=int) - 1
    if choice < 0 or choice >= len(ORIGIN_TYPES):
        click.echo(json.dumps({"error": "invalid selection"}))
        raise SystemExit(1)
    return ORIGIN_TYPES[choice]["key"]


def _collect_one_dbt_entry(
    entries: list[dict],
    tested: set[tuple[str, str | None]],
    profiles: list[dict],
) -> None:
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
