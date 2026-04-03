from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.integration.helper import (
    ANALYTICS_DIR,
    ECOMMERCE_DIR,
    SHARED_DB,
    compile_nagi_project,
    dbt_seed_and_run,
    write_nagi_project,
    write_profiles,
)

# ── dbt project fixtures ────────────────────────────────────────────────


@pytest.fixture(scope="session")
def dbt_ecommerce_ready() -> Path:
    """Seed and run the ecommerce dbt project."""
    assert ECOMMERCE_DIR.exists(), f"not found: {ECOMMERCE_DIR}"
    dbt_seed_and_run(ECOMMERCE_DIR)
    return ECOMMERCE_DIR


@pytest.fixture(scope="session")
def dbt_analytics_ready(dbt_ecommerce_ready: Path) -> Path:
    """Seed and run the analytics dbt project.

    Depends on ecommerce because analytics sources reference
    tables produced by ecommerce in the shared DuckDB.
    """
    assert ANALYTICS_DIR.exists(), f"not found: {ANALYTICS_DIR}"
    dbt_seed_and_run(ANALYTICS_DIR)
    return ANALYTICS_DIR


# ── nagi project fixtures ───────────────────────────────────────────────


@pytest.fixture(scope="session")
def compiled_project(
    dbt_analytics_ready: Path, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Session-scoped compiled nagi project with single Origin (analytics)."""
    tmp = tmp_path_factory.mktemp("compiled")
    profiles_dir = write_profiles(tmp / "profiles", {"analytics": "dev"})

    project_dir = tmp / "project"
    write_nagi_project(
        project_dir,
        [
            ("analytics", "analytics", dbt_analytics_ready, profiles_dir),
        ],
    )
    compile_nagi_project(project_dir)
    return project_dir


@pytest.fixture()
def nagi_project(dbt_analytics_ready: Path, tmp_path: Path) -> Path:
    """Function-scoped nagi project (not compiled)."""
    profiles_dir = write_profiles(tmp_path / "profiles", {"analytics": "dev"})

    project_dir = tmp_path / "project"
    write_nagi_project(
        project_dir,
        [
            ("analytics", "analytics", dbt_analytics_ready, profiles_dir),
        ],
    )
    return project_dir


@pytest.fixture(scope="session")
def multi_origin_project(
    dbt_analytics_ready: Path,
    dbt_ecommerce_ready: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Session-scoped compiled nagi project with two Origins."""
    tmp = tmp_path_factory.mktemp("multi")
    profiles_dir = write_profiles(
        tmp / "profiles", {"ecommerce": "dev", "analytics": "dev"}
    )

    project_dir = tmp / "project"
    write_nagi_project(
        project_dir,
        [
            ("ecommerce", "ecommerce", dbt_ecommerce_ready, profiles_dir),
            ("analytics", "analytics", dbt_analytics_ready, profiles_dir),
        ],
    )
    compile_nagi_project(project_dir)
    return project_dir


@pytest.fixture()
def duckdb_path(tmp_path: Path, dbt_analytics_ready: Path) -> Path:
    """Copy the shared DuckDB to tmp_path for isolated test use."""
    dst = tmp_path / "test.duckdb"
    if SHARED_DB.exists():
        shutil.copy2(SHARED_DB, dst)
    return dst
