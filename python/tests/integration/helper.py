from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from tests.helper import CMD_FALSE

DBT_PROJECTS_DIR = Path(__file__).parent.parent / "dbt_projects"
ANALYTICS_DIR = DBT_PROJECTS_DIR / "analytics"
ECOMMERCE_DIR = DBT_PROJECTS_DIR / "ecommerce"
SHARED_DB = DBT_PROJECTS_DIR / "dev.duckdb"

NOOP_SYNC = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Sync\n"
    "metadata:\n"
    "  name: reload\n"
    "spec:\n"
    "  run:\n"
    "    type: Command\n"
    '    args: ["echo", "sync"]\n'
)

SIMPLE_ASSET = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Asset\n"
    "metadata:\n"
    "  name: test-asset\n"
    "spec:\n"
    "  connection: test-duckdb\n"
)

DRIFTED_CONDITIONS = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Conditions\n"
    "metadata:\n"
    "  name: cmd-check\n"
    "spec:\n"
    "  - name: always-fail\n"
    "    type: Command\n"
    f"    run: [{CMD_FALSE}]\n"
)

DRIFTED_ASSET = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Asset\n"
    "metadata:\n"
    "  name: test-asset\n"
    "spec:\n"
    "  onDrift:\n"
    "    - conditions: cmd-check\n"
    "      sync: reload\n"
)


def dbt_seed_and_run(project_dir: Path, profiles_dir: str | Path = "profiles") -> None:
    """Run dbt seed + run for a project."""
    args_prefix = ["uv", "run", "dbt"]
    profiles_arg = ["--profiles-dir", str(profiles_dir)]
    subprocess.run(
        [*args_prefix, "seed", *profiles_arg],
        cwd=project_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [*args_prefix, "run", *profiles_arg],
        cwd=project_dir,
        check=True,
        capture_output=True,
    )


def write_profiles(dest: Path, profiles: dict[str, str]) -> Path:
    """Write a profiles.yml mapping profile names to the shared DuckDB."""
    dest.mkdir(parents=True, exist_ok=True)
    entries = []
    for name, target in profiles.items():
        entries.append(
            f"{name}:\n"
            f"  target: {target}\n"
            f"  outputs:\n"
            f"    {target}:\n"
            f"      type: duckdb\n"
            f"      path: {SHARED_DB}\n"
        )
    (dest / "profiles.yml").write_text("".join(entries))
    return dest


def write_nagi_project(
    project_dir: Path,
    origins: list[tuple[str, str, Path, Path]],
) -> None:
    """Create a nagi project with the given Origins.

    Each origin is (origin_name, profile_name, project_path, profiles_dir).
    """
    project_dir.mkdir(parents=True, exist_ok=True)
    resources_dir = project_dir / "resources"
    resources_dir.mkdir(exist_ok=True)

    state_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nstateDir: {state_dir}\n"
    )

    for origin_name, profile_name, dbt_project_path, profiles_dir in origins:
        conn_name = f"{origin_name}-dev"
        (resources_dir / f"connection-{origin_name}.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Connection\n"
            "metadata:\n"
            f"  name: {conn_name}\n"
            "spec:\n"
            "  type: dbt\n"
            f"  profile: {profile_name}\n"
            "  target: dev\n"
            f"  profilesDir: {profiles_dir}\n"
        )
        (resources_dir / f"origin-{origin_name}.yaml").write_text(
            "apiVersion: nagi.io/v1alpha1\n"
            "kind: Origin\n"
            "metadata:\n"
            f"  name: {origin_name}\n"
            "spec:\n"
            "  type: DBT\n"
            f"  connection: {conn_name}\n"
            f"  projectDir: {dbt_project_path}\n"
        )


def compile_nagi_project(project_dir: Path) -> None:
    """Run nagi compile and assert success."""
    result = run_nagi(
        [
            "compile",
            "--resources-dir",
            str(project_dir / "resources"),
            "--target-dir",
            str(project_dir / "target"),
            "--yes",
        ],
        cwd=project_dir,
    )
    assert result.returncode == 0, (
        f"compile failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )


def run_nagi(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run a nagi CLI command and return the CompletedProcess."""
    return subprocess.run(
        ["uv", "run", "nagi", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def run_nagi_json(args: list[str], cwd: Path) -> dict[str, Any] | list[Any]:
    """Run a nagi CLI command, assert success, and return parsed JSON output.

    Extracts the last JSON line from stdout to skip non-JSON output
    (e.g. dbt stderr leaking into stdout).
    """
    result = run_nagi(args, cwd)
    assert result.returncode == 0, (
        f"nagi {' '.join(args)} failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    lines = result.stdout.strip().splitlines()
    for line in reversed(lines):
        line = line.strip()
        if line.startswith(("{", "[")):
            return json.loads(line)
    return json.loads(result.stdout)


def write_duckdb_project(
    project_dir: Path,
    db_path: Path,
    resources: dict[str, str],
) -> None:
    """Write a nagi project with DuckDB direct connection."""
    project_dir.mkdir(exist_ok=True)
    resources_dir = project_dir / "resources"
    resources_dir.mkdir(exist_ok=True)

    state_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nstateDir: {state_dir}\n"
    )

    connection_yaml = (
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Connection\n"
        "metadata:\n"
        "  name: test-duckdb\n"
        "spec:\n"
        "  type: duckdb\n"
        f"  path: {db_path}\n"
    )
    (resources_dir / "connection.yaml").write_text(connection_yaml)

    for filename, content in resources.items():
        (resources_dir / filename).write_text(content)


def compile_project(project_dir: Path) -> dict[str, Any]:
    """Compile a nagi project and return the JSON output."""
    result = run_nagi_json(
        [
            "compile",
            "--resources-dir",
            str(project_dir / "resources"),
            "--target-dir",
            str(project_dir / "target"),
            "--yes",
        ],
        cwd=project_dir,
    )
    assert isinstance(result, dict)
    return result


def evaluate_project(
    project_dir: Path,
    select: str | None = None,
) -> list[Any]:
    """Evaluate a compiled project and return the result list."""
    args = [
        "evaluate",
        "--target-dir",
        str(project_dir / "target"),
    ]
    if select:
        args.extend(["--select", select])
    result = run_nagi_json(args, cwd=project_dir)
    assert isinstance(result, list)
    return result


def build_and_evaluate(
    tmp_path: Path,
    duckdb_path: Path,
    resources: dict[str, str],
    asset_name: str | None = None,
) -> dict[str, Any]:
    """Write, compile, evaluate a DuckDB project. Return first matching result."""
    project = tmp_path / "project"
    write_duckdb_project(project, duckdb_path, resources)
    compile_project(project)
    results = evaluate_project(project)
    if asset_name:
        return next(r for r in results if r["assetName"] == asset_name)
    return results[0]
