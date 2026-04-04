import sys
from pathlib import Path

# Resource names
CONNECTION_NAME = "my-bq"
SOURCE_NAME = "raw-sales"
ASSET_NAME = "daily-sales"
SYNC_NAME = "dbt-sync"
CONDITIONS_NAME = "freshness-check"

# Asset spec values
FRESHNESS_MAX_AGE = "24h"
FRESHNESS_INTERVAL = "6h"
FRESHNESS_COLUMN = "updated_at"

# Platform-dependent shell commands for YAML test fixtures.
# These are interpolated into kind: Conditions / kind: Sync YAML.
CMD_TRUE = "'powershell', '-Command', 'exit 0'" if sys.platform == "win32" else "'true'"
CMD_FALSE = (
    "'powershell', '-Command', 'exit 1'" if sys.platform == "win32" else "'false'"
)
ARGS_FALSE = (
    '["powershell", "-Command", "exit 1"]' if sys.platform == "win32" else '["false"]'
)
ARGS_SLEEP_2 = (
    '["powershell", "-Command", "Start-Sleep -Seconds 2"]'
    if sys.platform == "win32"
    else '["sleep", "2"]'
)

CONNECTION_YAML = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Connection\n"
    "metadata:\n"
    f"  name: {CONNECTION_NAME}\n"
    "spec:\n"
    "  type: dbt\n"
    "  profile: my_project\n"
)

UPSTREAM_ASSET_YAML = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Asset\n"
    "metadata:\n"
    f"  name: {SOURCE_NAME}\n"
    "spec:\n"
    f"  connection: {CONNECTION_NAME}\n"
)

CONDITIONS_YAML = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Conditions\n"
    "metadata:\n"
    f"  name: {CONDITIONS_NAME}\n"
    "spec:\n"
    "  - name: data-freshness\n"
    "    type: Freshness\n"
    f"    maxAge: {FRESHNESS_MAX_AGE}\n"
    f"    interval: {FRESHNESS_INTERVAL}\n"
    f"    column: {FRESHNESS_COLUMN}\n"
)

ASSET_YAML = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Asset\n"
    "metadata:\n"
    f"  name: {ASSET_NAME}\n"
    "spec:\n"
    f"  connection: {CONNECTION_NAME}\n"
    "  upstreams:\n"
    f"    - {SOURCE_NAME}\n"
    "  onDrift:\n"
    f"    - conditions: {CONDITIONS_NAME}\n"
    f"      sync: {SYNC_NAME}\n"
)

SYNC_YAML = (
    "apiVersion: nagi.io/v1alpha1\n"
    "kind: Sync\n"
    "metadata:\n"
    f"  name: {SYNC_NAME}\n"
    "spec:\n"
    "  run:\n"
    "    type: Command\n"
    '    args: ["dbt", "run"]\n'
)


def write_valid_resources(resources_dir: Path) -> None:
    """Write a minimal valid set of resource YAML files for testing."""
    resources_dir.mkdir()
    (resources_dir / "connection.yaml").write_text(CONNECTION_YAML)
    (resources_dir / "upstream.yaml").write_text(UPSTREAM_ASSET_YAML)
    (resources_dir / "conditions.yaml").write_text(CONDITIONS_YAML)
    (resources_dir / "asset.yaml").write_text(ASSET_YAML)
    (resources_dir / "sync.yaml").write_text(SYNC_YAML)


def yaml_args_touch(path: Path) -> str:
    """Return YAML args list that creates a file. Cross-platform."""
    p = path.as_posix()
    if sys.platform == "win32":
        return f'["powershell", "-Command", "New-Item -Force {p}"]'
    return f'["touch", "{p}"]'


def yaml_args_mkdir_and_touch(dir_path: Path, file_path: Path) -> str:
    """Return YAML args list that creates a directory and a file. Cross-platform."""
    d = dir_path.as_posix()
    f = file_path.as_posix()
    if sys.platform == "win32":
        return (
            f'["powershell", "-Command",'
            f' "New-Item -Force -ItemType Directory {d};'
            f' New-Item -Force {f}"]'
        )
    return f'["sh", "-c", "mkdir -p {d} && touch {f}"]'


def yaml_sync_cmd_mkdir_and_touch(dir_posix: str, file_template: str) -> str:
    """Return YAML args block for sync that creates a dir and touches a file.

    ``file_template`` may contain Nagi template variables like
    ``{{ asset.name }}``.
    """
    if sys.platform == "win32":
        return (
            "    args:\n"
            "      - powershell\n"
            "      - -Command\n"
            f'      - "New-Item -Force -ItemType Directory {dir_posix};'
            f' New-Item -Force {file_template}"\n'
        )
    return (
        "    args:\n"
        "      - sh\n"
        "      - -c\n"
        f'      - "mkdir -p {dir_posix}'
        f' && touch {file_template}"\n'
    )


def yaml_run_file_exists(path: Path) -> str:
    """Return YAML run list that checks if a file exists. Cross-platform."""
    p = path.as_posix()
    if sys.platform == "win32":
        return (
            f"['powershell', '-Command',"
            f" 'if (Test-Path {p}) {{ exit 0 }} else {{ exit 1 }}']"
        )
    return f"['test', '-f', '{p}']"
