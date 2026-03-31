from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

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
    "    run: ['false']\n"
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

    nagi_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nnagiDir: {nagi_dir}\n"
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
    cache_dir: Path | None = None,
) -> list[Any]:
    """Evaluate a compiled project and return the result list."""
    resolved_cache = cache_dir or project_dir / "cache"
    args = [
        "evaluate",
        "--target-dir",
        str(project_dir / "target"),
        "--cache-dir",
        str(resolved_cache),
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
