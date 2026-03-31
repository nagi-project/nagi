from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


class TestInit:
    def test_init_without_dbt(self, tmp_path: Path) -> None:
        """init creates resources/ directory without dbt setup."""
        project = tmp_path / "project"
        project.mkdir()

        result = subprocess.run(
            ["uv", "run", "nagi", "init"],
            input="n\n",
            capture_output=True,
            text=True,
            cwd=project,
        )
        assert result.returncode == 0, (
            f"init failed:\nstdout={result.stdout}\nstderr={result.stderr}"
        )
        assert (project / "resources").is_dir()

    def test_init_is_idempotent(self, tmp_path: Path) -> None:
        """Running init twice does not fail or overwrite existing resources."""
        project = tmp_path / "project"
        project.mkdir()

        for _ in range(2):
            result = subprocess.run(
                ["uv", "run", "nagi", "init"],
                input="n\n",
                capture_output=True,
                text=True,
                cwd=project,
            )
            assert result.returncode == 0
        assert (project / "resources").is_dir()
