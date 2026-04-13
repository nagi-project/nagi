from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from generate_sitemap_index import build_sitemap_index

NAMESPACE = "http://www.sitemaps.org/schemas/sitemap/0.9"


def _parse_locs(xml_str: str) -> list[str]:
    """Extract all <loc> text values from a sitemap index XML string."""
    root = ET.fromstring(xml_str)
    return [loc.text for loc in root.findall(f"{{{NAMESPACE}}}sitemap/{{{NAMESPACE}}}loc")]


@pytest.mark.parametrize(
    ("versions", "expected_locs"),
    [
        pytest.param(
            ["0.1"],
            [
                "https://nagi-project.dev/0.1/sitemap.xml",
                "https://nagi-project.dev/ja/0.1/sitemap.xml",
            ],
            id="single_version",
        ),
        pytest.param(
            ["0.2", "0.1"],
            [
                "https://nagi-project.dev/0.2/sitemap.xml",
                "https://nagi-project.dev/ja/0.2/sitemap.xml",
                "https://nagi-project.dev/0.1/sitemap.xml",
                "https://nagi-project.dev/ja/0.1/sitemap.xml",
            ],
            id="multiple_versions_preserves_order",
        ),
    ],
)
def test_build_sitemap_index(versions: list[str], expected_locs: list[str]) -> None:
    xml_str = build_sitemap_index(versions)
    assert _parse_locs(xml_str) == expected_locs


def test_xml_format() -> None:
    xml_str = build_sitemap_index(["0.1"])
    assert xml_str.startswith('<?xml version="1.0" encoding="UTF-8"?>')
    root = ET.fromstring(xml_str)
    assert root.tag == f"{{{NAMESPACE}}}sitemapindex"
    assert xml_str.endswith("\n")
