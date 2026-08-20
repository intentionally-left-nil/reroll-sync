"""HTTP client for the PyPI simple index JSON API.

See https://docs.pypi.org/api/index-api/ for the API this wraps.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

SIMPLE_INDEX_URL = "https://pypi.org/simple/"
ACCEPT_HEADER = "application/vnd.pypi.simple.v1+json"


@dataclass(frozen=True)
class IndexProject:
    """A single project entry from the ``/simple/`` index."""

    name: str
    serial: int


@dataclass(frozen=True)
class SimpleIndexResponse:
    """The parsed response of the ``/simple/`` index endpoint."""

    last_serial: int
    projects: tuple[IndexProject, ...]


@dataclass(frozen=True)
class ProjectFile:
    """A single file entry from a project's ``/simple/{name}/`` page.

    ``raw`` is the file's untouched JSON object, suitable for storing as-is
    in the ``wheels.pypi_simple`` column.
    """

    filename: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class ProjectResponse:
    """The parsed response of a project's ``/simple/{name}/`` page."""

    last_serial: int
    files: tuple[ProjectFile, ...]


def fetch_simple_index(timeout: float | None = None) -> SimpleIndexResponse:
    """Fetch and parse the PyPI ``/simple/`` project index."""
    data = _get_json(SIMPLE_INDEX_URL, timeout)
    projects = tuple(
        IndexProject(name=project["name"], serial=project["_last-serial"])
        for project in data["projects"]
    )
    return SimpleIndexResponse(last_serial=data["meta"]["_last-serial"], projects=projects)


def fetch_project(name: str, timeout: float | None = None) -> ProjectResponse:
    """Fetch and parse the PyPI ``/simple/{name}/`` file listing for a project."""
    url = f"{SIMPLE_INDEX_URL}{urllib.parse.quote(name, safe='')}/"
    data = _get_json(url, timeout)
    files = tuple(ProjectFile(filename=file["filename"], raw=file) for file in data["files"])
    return ProjectResponse(last_serial=data["meta"]["_last-serial"], files=files)


def _get_json(url: str, timeout: float | None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": ACCEPT_HEADER})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)
