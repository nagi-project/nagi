from __future__ import annotations

from collections.abc import Callable, Generator
from pathlib import Path

import pytest

from tests.scenario.helper import init_nagi_dir, start_serve, stop_serve, write_project

StartServe = Callable[[dict[str, str]], Path]


@pytest.fixture()
def serve_project(tmp_path: Path) -> Path:
    """Return a tmp project directory."""
    return tmp_path / "project"


@pytest.fixture()
def run_serve(serve_project: Path) -> Generator[StartServe]:
    """Fixture that starts serve and stops it after test."""
    proc = None

    def _start(resources: dict[str, str]) -> Path:
        nonlocal proc
        write_project(serve_project, resources)
        init_nagi_dir(serve_project)
        proc = start_serve(serve_project)
        return serve_project

    yield _start

    if proc is not None:
        stop_serve(proc)
