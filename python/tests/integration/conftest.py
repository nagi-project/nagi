from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from tests.integration.helper import run_nagi

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"


@pytest.fixture(scope="session")
def dbt_project_ready() -> Path:
    """Ensure dbt project has been seeded and compiled. Returns project path."""
    assert DBT_PROJECT_DIR.exists(), f"dbt project not found: {DBT_PROJECT_DIR}"
    subprocess.run(
        ["uv", "run", "dbt", "seed", "--profiles-dir", "profiles"],
        cwd=DBT_PROJECT_DIR,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["uv", "run", "dbt", "run", "--profiles-dir", "profiles"],
        cwd=DBT_PROJECT_DIR,
        check=True,
        capture_output=True,
    )
    return DBT_PROJECT_DIR


@pytest.fixture(scope="session")
def compiled_project(
    dbt_project_ready: Path, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Session-scoped compiled nagi project with dbt Origin.

    Compile runs once. Tests share target/ (read-only) but use their own
    cache_dir via test-level tmp_path.
    """
    tmp = tmp_path_factory.mktemp("compiled")
    project_dir = tmp / "project"
    project_dir.mkdir()

    resources_dir = project_dir / "resources"
    resources_dir.mkdir()

    nagi_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nnagiDir: {nagi_dir}\n"
    )

    abs_duckdb_path = dbt_project_ready / "dev.duckdb"
    profiles_dir = tmp / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        "integration_test:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {abs_duckdb_path}\n"
    )

    (resources_dir / "connection.yaml").write_text(
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Connection\n"
        "metadata:\n"
        "  name: integration-test-dev\n"
        "spec:\n"
        "  type: dbt\n"
        "  profile: integration_test\n"
        "  target: dev\n"
        f"  profilesDir: {profiles_dir}\n"
    )
    (resources_dir / "origin.yaml").write_text(
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Origin\n"
        "metadata:\n"
        "  name: integration-test\n"
        "spec:\n"
        "  type: DBT\n"
        "  connection: integration-test-dev\n"
        f"  projectDir: {dbt_project_ready}\n"
    )

    result = run_nagi(
        [
            "compile",
            "--resources-dir",
            str(resources_dir),
            "--target-dir",
            str(project_dir / "target"),
            "--yes",
        ],
        cwd=project_dir,
    )
    assert result.returncode == 0, (
        f"compile failed:\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    return project_dir


@pytest.fixture()
def nagi_project(dbt_project_ready: Path, tmp_path: Path) -> Path:
    """Function-scoped nagi project (not compiled). For tests that need
    to write additional resources before compiling."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    resources_dir = project_dir / "resources"
    resources_dir.mkdir()

    nagi_dir = project_dir / ".nagi"
    (project_dir / "nagi.yaml").write_text(
        f"resourcesDir: resources\nnagiDir: {nagi_dir}\n"
    )

    abs_duckdb_path = dbt_project_ready / "dev.duckdb"
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        "integration_test:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {abs_duckdb_path}\n"
    )

    (resources_dir / "connection.yaml").write_text(
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Connection\n"
        "metadata:\n"
        "  name: integration-test-dev\n"
        "spec:\n"
        "  type: dbt\n"
        "  profile: integration_test\n"
        "  target: dev\n"
        f"  profilesDir: {profiles_dir}\n"
    )
    (resources_dir / "origin.yaml").write_text(
        "apiVersion: nagi.io/v1alpha1\n"
        "kind: Origin\n"
        "metadata:\n"
        "  name: integration-test\n"
        "spec:\n"
        "  type: DBT\n"
        "  connection: integration-test-dev\n"
        f"  projectDir: {dbt_project_ready}\n"
    )

    return project_dir


@pytest.fixture()
def duckdb_path(tmp_path: Path, dbt_project_ready: Path) -> Path:
    """Return the path to the seeded DuckDB database (copied to tmp_path)."""
    src = dbt_project_ready / "dev.duckdb"
    dst = tmp_path / "test.duckdb"
    if src.exists():
        shutil.copy2(src, dst)
    return dst
