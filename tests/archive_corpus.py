"""Synthetic ~METADATA-shaped text corpus, for archive compression tests.

Not a ``test_*`` module itself -- imported by ``test_archive_compression.py``
to build a realistic-ish corpus without depending on real PyPI data.
"""

import random

_COMMON_WORDS = [
    "the",
    "a",
    "an",
    "and",
    "or",
    "but",
    "if",
    "then",
    "else",
    "for",
    "while",
    "with",
    "without",
    "into",
    "onto",
    "over",
    "under",
    "above",
    "below",
    "between",
    "among",
    "through",
    "across",
    "along",
    "around",
    "data",
    "set",
    "list",
    "dict",
    "object",
    "class",
    "function",
    "method",
    "module",
    "package",
    "system",
    "service",
    "client",
    "server",
    "request",
    "response",
    "error",
    "value",
    "type",
    "support",
    "provide",
    "allow",
    "enable",
    "ensure",
    "improve",
    "reduce",
    "increase",
    "optimize",
    "python",
    "interface",
    "implementation",
    "library",
    "standard",
    "simple",
    "fast",
    "easy",
]

_LICENSES = ("MIT", "BSD-3-Clause", "Apache-2.0", "GPL-3.0-or-later", "MPL-2.0")

_CLASSIFIERS = (
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Operating System :: OS Independent",
    "Topic :: Software Development :: Libraries",
)


def generate_corpus(
    *, n_projects: int = 50, versions_per_project: int = 20, seed: int = 1234
) -> list[bytes]:
    """Return ``n_projects * versions_per_project`` records, in project order.

    Each record is shaped like a real ``.dist-info/METADATA`` file: PEP 566
    headers plus a description body. Versions of the same project share
    most of their description (simulating near-identical consecutive
    METADATA), but each project draws from its own vocabulary slice so
    unrelated projects are not gratuitously similar to each other.
    """
    rng = random.Random(seed)
    universe = [
        "".join(rng.choices("abcdefghijklmnopqrstuvwxyz", k=rng.randint(4, 9)))
        for _ in range(n_projects * 130)
    ]
    records = []
    for project_index in range(n_projects):
        records.extend(_project_records(rng, project_index, versions_per_project, universe))
    return records


def _project_records(rng, project_index, versions_per_project, universe):
    vocab = _COMMON_WORDS + rng.sample(universe, 120)
    name = f"pkg-{project_index:04d}-{rng.choice(universe)}"
    org = f"org{project_index % 37}"
    author = f"Author {project_index % 211}"
    email = f"author{project_index % 211}@example.com"
    license_name = _LICENSES[project_index % len(_LICENSES)]
    description = _paragraph(rng, vocab, 30)
    deps = [f"{rng.choice(universe)}-lib{d}" for d in range(rng.randint(2, 10))]

    records = []
    for version_index in range(1, versions_per_project + 1):
        version = f"{1 + version_index // 10}.{version_index % 10}.{rng.randint(0, 9)}"
        if rng.random() < 0.3 and deps:
            deps = [*deps, f"{rng.choice(universe)}-lib{len(deps)}"]
        records.append(
            _metadata_body(
                rng=rng,
                vocab=vocab,
                name=name,
                version=version,
                org=org,
                author=author,
                email=email,
                license_name=license_name,
                deps=deps,
                description=description,
            )
        )
    return records


def _metadata_body(
    *, rng, vocab, name, version, org, author, email, license_name, deps, description
):
    changelog = _paragraph(rng, vocab, 6)
    classifiers = "\n".join(f"Classifier: {c}" for c in _CLASSIFIERS)
    requires = "\n".join(f"Requires-Dist: {d}" for d in deps)
    body = (
        f"Metadata-Version: 2.1\n"
        f"Name: {name}\n"
        f"Version: {version}\n"
        f"Summary: A package for {rng.choice(vocab)} {rng.choice(vocab)}\n"
        f"Home-page: https://github.com/{org}/{name}\n"
        f"Author: {author}\n"
        f"Author-email: {email}\n"
        f"License: {license_name}\n"
        f"{classifiers}\n"
        f"{requires}\n"
        f"Description-Content-Type: text/markdown\n"
        f"\n"
        f"{description}\n\n"
        f"Changelog for {version}: {changelog}\n"
    )
    return body.encode()


def _paragraph(rng, vocab, n_sentences):
    return " ".join(_sentence(rng, vocab, rng.randint(8, 16)) for _ in range(n_sentences))


def _sentence(rng, vocab, n_words):
    words = [rng.choice(vocab) for _ in range(n_words)]
    words[0] = words[0].capitalize()
    return " ".join(words) + "."
