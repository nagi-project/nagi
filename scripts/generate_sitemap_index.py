"""Generate a sitemap index for the docs site.

Usage:
    uv run scripts/generate_sitemap_index.py <site_dir>

Reads versions.json from <site_dir> to discover versioned sitemaps, then
writes a sitemap index XML file to <site_dir>/sitemap.xml referencing both
English and Japanese sitemaps for each version.
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SITE_URL = "https://nagi-project.dev"
NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"


def build_sitemap_index(versions: list[str]) -> str:
    """Return a sitemap index XML string for the given version list."""
    root = ET.Element("sitemapindex", xmlns=NAMESPACE)

    for version in versions:
        for prefix in [f"{version}", f"ja/{version}"]:
            sitemap = ET.SubElement(root, "sitemap")
            loc = ET.SubElement(sitemap, "loc")
            loc.text = f"{SITE_URL}/{prefix}/sitemap.xml"

    ET.indent(root)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        root, encoding="unicode"
    ) + "\n"


def generate_sitemap_index(site_dir: Path) -> None:
    versions_path = site_dir / "versions.json"

    with versions_path.open() as f:
        raw: list[dict[str, object]] = json.load(f)

    versions = [str(entry["version"]) for entry in raw]
    xml = build_sitemap_index(versions)

    with (site_dir / "sitemap.xml").open("w") as f:
        f.write(xml)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <site_dir>", file=sys.stderr)
        sys.exit(1)

    site_dir = Path(sys.argv[1])

    if not site_dir.is_dir():
        print(f"Error: {site_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    if not (site_dir / "versions.json").exists():
        print(f"Error: {site_dir / 'versions.json'} not found", file=sys.stderr)
        sys.exit(1)

    generate_sitemap_index(site_dir)
    print(f"Generated {site_dir / 'sitemap.xml'}")


if __name__ == "__main__":
    main()
