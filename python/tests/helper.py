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
