import pytest

from update_versions import VersionEntry, build_redirect, build_versions


def _v(version: str, is_latest: bool = False) -> VersionEntry:
    return VersionEntry(version=version, is_latest=is_latest)


@pytest.mark.parametrize(
    "existing,minor,expected",
    [
        pytest.param(
            [],
            "0.0",
            [_v("0.0", True)],
            id="first-version",
        ),
        pytest.param(
            [_v("0.0", True)],
            "0.1",
            [_v("0.1", True), _v("0.0")],
            id="add-new-version",
        ),
        pytest.param(
            [_v("0.1", True), _v("0.0")],
            "0.1",
            [_v("0.1", True), _v("0.0")],
            id="update-existing-version",
        ),
        pytest.param(
            [_v("0.2", True), _v("0.1"), _v("0.0")],
            "0.3",
            [_v("0.3", True), _v("0.2"), _v("0.1"), _v("0.0")],
            id="add-third-version",
        ),
        pytest.param(
            [_v("0.2", True), _v("0.1"), _v("0.0")],
            "0.0",
            [_v("0.2", True), _v("0.1"), _v("0.0")],
            id="rebuild-old-version",
        ),
        pytest.param(
            [_v("0.2", True), _v("0.0")],
            "0.1",
            [_v("0.2", True), _v("0.1"), _v("0.0")],
            id="add-old-version",
        ),
    ],
)
def test_build_versions(existing, minor, expected):
    result = build_versions(existing, minor)
    assert result == expected


@pytest.mark.parametrize(
    "versions,expected_url",
    [
        pytest.param(
            [_v("0.1", True)],
            "/0.1/",
            id="single-version",
        ),
        pytest.param(
            [_v("0.2", True), _v("0.1")],
            "/0.2/",
            id="latest-is-highest",
        ),
        pytest.param(
            [_v("0.2"), _v("0.1")],
            "/0.2/",
            id="no-latest-falls-back-to-first",
        ),
    ],
)
def test_build_redirect(versions, expected_url):
    html = build_redirect(versions)
    assert f"url={expected_url}" in html
    assert f'href="{expected_url}"' in html


def test_version_entry_to_dict_latest():
    entry = _v("0.1", True)
    assert entry.to_dict() == {
        "version": "0.1",
        "title": "0.1 (latest)",
        "aliases": ["latest"],
    }


def test_version_entry_to_dict_not_latest():
    entry = _v("0.1")
    assert entry.to_dict() == {
        "version": "0.1",
        "title": "0.1",
        "aliases": [],
    }


def test_version_entry_from_dict():
    d = {"version": "0.1", "title": "0.1 (latest)", "aliases": ["latest"]}
    entry = VersionEntry.from_dict(d)
    assert entry == _v("0.1", True)


def test_build_versions_does_not_mutate_input():
    existing = [_v("0.0", True)]
    build_versions(existing, "0.1")
    assert existing == [_v("0.0", True)]


def test_build_redirect_empty_raises():
    with pytest.raises(ValueError, match="must not be empty"):
        build_redirect([])


@pytest.mark.parametrize(
    "invalid_version",
    [
        pytest.param("", id="empty"),
        pytest.param("1", id="single-number"),
        pytest.param("abc", id="non-numeric"),
        pytest.param("1.2.3", id="three-parts"),
    ],
)
def test_build_versions_invalid_version_raises(invalid_version):
    with pytest.raises(ValueError, match="invalid minor version"):
        build_versions([], invalid_version)
