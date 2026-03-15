import json
import subprocess
from pathlib import Path

import click
import yaml

from nagi_cli._nagi_core import load_dbt_profiles


@click.command()
def init() -> None:
    """Prepare the environment so that `nagi compile` can run."""
    _ensure_assets_dir()
    _ensure_config()

    if not click.confirm("Do you use dbt?", default=False):
        return

    profiles_json = _load_profiles()
    profiles = json.loads(profiles_json)["profiles"]
    _setup_dbt_projects(profiles)


def _load_profiles() -> str:
    try:
        return load_dbt_profiles()
    except RuntimeError as e:
        click.echo(json.dumps({"error": str(e)}))
        raise SystemExit(1)


def _select_profile_target(
    profiles: list[dict],
) -> tuple[str, str]:
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


def _test_connection(dbt_dir: Path, profile: str, target: str | None) -> None:
    cmd = ["dbt", "debug", "--project-dir", str(dbt_dir), "--profile", profile]
    if target:
        cmd.extend(["--target", target])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        click.echo(json.dumps({"error": "dbt command not found"}))
        raise SystemExit(1)
    if result.returncode != 0:
        click.echo(json.dumps({"error": f"dbt debug failed: {result.stdout}"}))
        raise SystemExit(1)
    click.echo(json.dumps({"connection": "ok"}))


def _ensure_assets_dir() -> None:
    assets_dir = Path("assets")
    assets_dir.mkdir(exist_ok=True)


def _ensure_config() -> None:
    config_dir = Path.home() / ".nagi"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "config.yaml"
    if not config_path.exists():
        config_path.write_text("backend:\n  type: local\n")


def _connection_name(profile: str, target: str | None) -> str:
    return f"{profile}-{target}" if target else profile


def _build_connection_yaml(profile: str, target: str | None) -> str:
    name = _connection_name(profile, target)
    target_line = f"\n    target: {target}" if target else ""
    return (
        f"apiVersion: nagi.io/v1alpha1\n"
        f"kind: Connection\n"
        f"metadata:\n"
        f"  name: {name}\n"
        f"spec:\n"
        f"  dbtProfile:\n"
        f"    profile: {profile}{target_line}\n"
    )


def _setup_dbt_projects(profiles: list[dict]) -> None:
    origin_path = Path("assets") / "origin.yaml"
    if origin_path.exists():
        return

    connection_path = Path("assets") / "connection.yaml"

    origins: list[str] = []
    connections: dict[str, str] = {}
    tested: set[tuple[str, str | None]] = set()

    while True:
        dbt_dir = Path(click.prompt("Path to dbt project directory"))
        profile_name, target = _select_profile_target(profiles)
        conn_name = _connection_name(profile_name, target)

        if (profile_name, target) not in tested:
            _test_connection(dbt_dir, profile_name, target)
            tested.add((profile_name, target))
            connections[conn_name] = _build_connection_yaml(profile_name, target)

        origin = _read_dbt_origin(dbt_dir, conn_name)
        origins.append(origin)

        if not click.confirm("Do you have another dbt project?", default=False):
            break

    if connections and not connection_path.exists():
        connection_path.write_text("---\n".join(connections.values()))
        click.echo(json.dumps({"connection": str(connection_path)}))

    if origins:
        origin_path.write_text("---\n".join(origins))
        click.echo(json.dumps({"origin": str(origin_path)}))


def _read_dbt_origin(dbt_dir: Path, connection_name: str) -> str:
    dbt_project = dbt_dir / "dbt_project.yml"
    if not dbt_project.exists():
        click.echo(json.dumps({"error": f"dbt_project.yml not found in {dbt_dir}"}))
        raise SystemExit(1)

    with open(dbt_project) as f:
        dbt_config = yaml.safe_load(f)
    project_name = dbt_config.get("name", "my-dbt-project")
    return (
        f"apiVersion: nagi.io/v1alpha1\n"
        f"kind: Origin\n"
        f"metadata:\n"
        f"  name: {project_name}\n"
        f"spec:\n"
        f"  type: DBT\n"
        f"  connection: {connection_name}\n"
        f"  projectDir: {dbt_dir}\n"
    )
