import json
from pathlib import Path

import click

from nagi_cli._nagi_core import load_dbt_profiles, test_connection


@click.command()
def init() -> None:
    """Prepare the environment so that `nagi compile` can run."""
    profiles_json = _load_profiles()
    if profiles_json is None:
        return

    profiles = json.loads(profiles_json)["profiles"]
    profile, target = _select_profile_target(profiles)
    if profile is None:
        return

    _test_connection(profile, target)
    _ensure_assets_dir()
    _ensure_config()
    _detect_dbt_project()


def _load_profiles() -> str | None:
    try:
        result = load_dbt_profiles()
        return result
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


def _select_profile_target(
    profiles: list[dict],
) -> tuple[str | None, str | None]:
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


def _test_connection(profile: str, target: str | None) -> None:
    try:
        result_json = test_connection(profile, target)
        result = json.loads(result_json)
        click.echo(json.dumps({"connection": result}))
    except RuntimeError as e:
        click.echo(json.dumps({"error": f"connection failed: {e}"}))
        raise SystemExit(1)


def _ensure_assets_dir() -> None:
    assets_dir = Path("assets")
    assets_dir.mkdir(exist_ok=True)


def _ensure_config() -> None:
    config_dir = Path.home() / ".nagi"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "config.yaml"
    if not config_path.exists():
        config_path.write_text("backend:\n  type: local\n")


def _detect_dbt_project() -> None:
    dbt_project = Path("dbt_project.yml")
    if not dbt_project.exists():
        return
    origin_path = Path("assets") / "origin.yaml"
    if origin_path.exists():
        return
    # Read project name from dbt_project.yml.
    import yaml  # noqa: F811 -- delayed import, only needed when dbt project exists

    with open(dbt_project) as f:
        dbt_config = yaml.safe_load(f)
    project_name = dbt_config.get("name", "my-dbt-project")
    origin_yaml = (
        f"kind: Origin\nmetadata:\n  name: {project_name}\nspec:\n  type: DBT\n"
    )
    origin_path.write_text(origin_yaml)
    click.echo(json.dumps({"origin": str(origin_path)}))
