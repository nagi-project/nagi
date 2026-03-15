from pathlib import Path

CONNECTION_YAML = (
    "kind: Connection\n"
    "metadata:\n"
    "  name: my-bq\n"
    "spec:\n"
    "  dbtProfile:\n"
    "    profile: my_project\n"
)

SOURCE_YAML = "kind: Source\nmetadata:\n  name: raw-sales\nspec:\n  connection: my-bq\n"

ASSET_YAML = (
    "kind: Asset\n"
    "metadata:\n"
    "  name: daily-sales\n"
    "spec:\n"
    "  sources:\n"
    "    - ref: raw-sales\n"
    "  sync:\n"
    "    ref: dbt-sync\n"
)

SYNC_YAML = (
    "kind: Sync\n"
    "metadata:\n"
    "  name: dbt-sync\n"
    "spec:\n"
    "  run:\n"
    "    type: Command\n"
    '    args: ["dbt", "run"]\n'
)


def write_valid_assets(assets_dir: Path) -> None:
    """Write a minimal valid set of asset YAML files for testing."""
    assets_dir.mkdir()
    (assets_dir / "connection.yaml").write_text(CONNECTION_YAML)
    (assets_dir / "source.yaml").write_text(SOURCE_YAML)
    (assets_dir / "asset.yaml").write_text(ASSET_YAML)
    (assets_dir / "sync.yaml").write_text(SYNC_YAML)
