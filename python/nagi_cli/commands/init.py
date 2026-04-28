import json

import click

from nagi_cli._nagi_core import (
    InitConfigResult,
    OriginType,
    init_workspace,
    load_dbt_profiles,
    run_dbt_debug,
    write_init_dbt_files,
)

DOCS_URL = "https://nagi-project.dev"

ORIGIN_TYPES = [OriginType.Dbt]


@click.command()
@click.option(
    "--overwrite-remote",
    is_flag=True,
    default=False,
    help="Overwrite existing remote project config.",
)
def init(overwrite_remote: bool) -> None:
    """Prepare the environment so that `nagi compile` can run."""
    _init_workspace(overwrite_remote)
    entries = _collect_origin_entries()
    if entries:
        _write_origin_files(entries)


def _init_workspace(overwrite_remote: bool) -> None:
    try:
        result = init_workspace(force=overwrite_remote)
        if result == InitConfigResult.Skipped:
            click.echo(
                "Remote project config already exists."
                " Use --overwrite-remote to overwrite."
            )
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


def _collect_origin_entries() -> list[dict]:
    if not click.confirm("Set up an Origin?", default=True):
        click.echo(f"See {DOCS_URL} for manual configuration.")
        return []

    entries: list[dict] = []
    tested: set[tuple[str, str | None]] = set()
    profiles: list[dict] | None = None

    while True:
        origin_type = _select_origin_type()

        if origin_type == OriginType.Dbt:
            if profiles is None:
                profiles = _load_dbt_profiles()
            _collect_one_dbt_entry(entries, tested, profiles)

        if not click.confirm("Add another Origin?", default=False):
            break

    return entries


def _write_origin_files(entries: list[dict]) -> None:
    try:
        result = json.loads(
            write_init_dbt_files(base_dir=".", entries_json=json.dumps(entries))
        )
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)

    if result.get("connectionPath"):
        click.echo(json.dumps({"connection": result["connectionPath"]}))
    if result.get("originPath"):
        click.echo(json.dumps({"origin": result["originPath"]}))


def _load_dbt_profiles() -> list[dict]:
    try:
        return json.loads(load_dbt_profiles())["profiles"]
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


def _select_origin_type() -> OriginType:
    click.echo("Origin types:")
    for i, ot in enumerate(ORIGIN_TYPES):
        click.echo(f"  {i + 1}) {ot.label}")
    choice = click.prompt("Select Origin type", type=int) - 1
    if choice < 0 or choice >= len(ORIGIN_TYPES):
        click.echo(json.dumps({"error": "invalid selection"}))
        raise SystemExit(1)
    return ORIGIN_TYPES[choice]


def _collect_one_dbt_entry(
    entries: list[dict],
    tested: set[tuple[str, str | None]],
    profiles: list[dict],
) -> None:
    dbt_dir = click.prompt("Path to dbt project directory")
    profile_name, target = _select_profile_target(profiles)

    if (profile_name, target) not in tested:
        try:
            run_dbt_debug(project_dir=dbt_dir, profile=profile_name, target=target)
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
