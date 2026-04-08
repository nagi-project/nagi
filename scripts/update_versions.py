"""Update versions.json and root redirect for MkDocs Material version selector.

Usage:
    uv run scripts/update_versions.py <site_dir> <minor_version>

Reads existing versions.json from <site_dir> (if present), adds or updates
the given minor version, marks it as latest, and writes back. Also generates
a root index.html that redirects to the latest version.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

_MINOR_VERSION_RE = re.compile(r"^\d+\.\d+$")


@dataclass(frozen=True)
class VersionEntry:
    version: str
    is_latest: bool

    @property
    def title(self) -> str:
        return f"{self.version} (latest)" if self.is_latest else self.version

    @property
    def aliases(self) -> list[str]:
        return ["latest"] if self.is_latest else []

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "title": self.title,
            "aliases": self.aliases,
        }

    @staticmethod
    def from_dict(d: dict[str, object]) -> VersionEntry:
        return VersionEntry(
            version=str(d["version"]),
            is_latest="latest" in d.get("aliases", []),
        )


def _parse_minor(version: str) -> tuple[int, int]:
    if not _MINOR_VERSION_RE.match(version):
        raise ValueError(f"invalid minor version: '{version}' (expected N.N)")
    parts = version.split(".")
    return (int(parts[0]), int(parts[1]))


def build_versions(
    existing: list[VersionEntry], minor_version: str
) -> list[VersionEntry]:
    """Return an updated version list with minor_version added or updated."""
    others = [e for e in existing if e.version != minor_version]

    current = _parse_minor(minor_version)
    is_latest = all(_parse_minor(e.version) <= current for e in others)

    new_entry = VersionEntry(version=minor_version, is_latest=is_latest)

    if is_latest:
        demoted = [VersionEntry(version=e.version, is_latest=False) for e in others]
        return [new_entry, *demoted]

    result = [*others, new_entry]
    result.sort(key=lambda e: _parse_minor(e.version), reverse=True)
    return result


def build_redirect(versions: list[VersionEntry]) -> str:
    """Return an HTML redirect page pointing to the latest version."""
    if not versions:
        raise ValueError("versions must not be empty")
    latest = next(
        (e.version for e in versions if e.is_latest),
        versions[0].version,
    )
    return (
        "<!DOCTYPE html>\n"
        "<html>\n"
        f'<head><meta http-equiv="refresh" content="0; url=/{latest}/"></head>\n'
        f'<body><a href="/{latest}/">Redirecting...</a></body>\n'
        "</html>\n"
    )


def update_versions(site_dir: Path, minor_version: str) -> None:
    versions_path = site_dir / "versions.json"

    if versions_path.exists():
        with versions_path.open() as f:
            raw: list[dict[str, object]] = json.load(f)
        existing = [VersionEntry.from_dict(d) for d in raw]
    else:
        existing = []

    versions = build_versions(existing, minor_version)

    with versions_path.open("w") as f:
        json.dump([v.to_dict() for v in versions], f, indent=2)
        f.write("\n")

    with (site_dir / "index.html").open("w") as f:
        f.write(build_redirect(versions))


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <site_dir> <minor_version>", file=sys.stderr)
        sys.exit(1)

    site_dir = Path(sys.argv[1])
    minor_version = sys.argv[2]

    if not site_dir.is_dir():
        print(f"Error: {site_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    if not _MINOR_VERSION_RE.match(minor_version):
        print(
            f"Error: invalid minor version '{minor_version}' (expected N.N)",
            file=sys.stderr,
        )
        sys.exit(1)

    update_versions(site_dir, minor_version)
    print(f"Updated {site_dir / 'versions.json'} with version {minor_version}")


if __name__ == "__main__":
    main()
