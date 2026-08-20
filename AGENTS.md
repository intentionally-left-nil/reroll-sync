# AGENTS.md

reroll-sync builds and maintains a local sqlite mirror of PyPI wheel
metadata (the simple index, per-project file listings`METADATA`/repodata)
so that reroll has a fast, offline source of truth to work from instead of hitting PyPI directly.

## Workflow: tests before code

Every edge case gets a test *before* the code that handles it exists.

1. New edge case? Write a unit test for the specific sub-behavior (e.g. a
   single schema-validation rule, a single PyPI response quirk, a single
   ingestion-algorithm step) *before* implementing it. Prefer small,
   targeted tests over broad end-to-end ones.
2. New test starts failing (or `xfail` if it can't even run yet). Run
   `make test` to confirm it fails for the right reason.
3. Implement the minimum needed to make it pass. A test is never edited to
   match the implementation's output.

Some amount of `uv run python3 -c ...` exploration is fine for quick,
throwaway sanity checks — e.g. confirming a value you already computed, or
checking basic Python syntax. It is *not* fine for exploring how code you
did not just write behaves — stdlib, a dependency, reroll-sync's own
existing code, the real PyPI API, anything. The test is purpose, not
package name: are you running this to *discover* what some input produces
(an edge case, a PRAGMA quirk, "what does this raise for X?", "what does
the real-world PyPI response actually look like?") rather than to confirm
something you already know the answer to? If discovery is the point, stop
before running it and write that exploration as a unit test in `tests/`
instead, then run it with `make test`. A one-liner is not a lighter-weight
version of that test — its output vanishes, so it has to be redone (and
re-discovered) the next time the same question comes up. The test is the
durable, re-runnable version of the same few lines of code, and it
directly grows the edge-case corpus the codebase is trying to build.

## Commands

- `make install` — sync deps (uv)
- `make test` — unit tests (with coverage; fails if coverage < 100%)
- `make lint` / `make format` — ruff
- `make typecheck` — ty
- `make ci` — everything CI runs; run this before opening a PR
- `make run ARGS="..."` — run the `reroll_sync` CLI
- `make init [DB_PATH=...]` — run `reroll_sync init` against a db path

## Code conventions

- Package code lives in `src/reroll_sync/`; tests in `tests/`.
- Dev-only deps go in `[dependency-groups.dev]`.
- Python 3.13+, fully typed. `ty` and `ruff` must pass with zero warnings.
- Coverage must stay at 100% (`[tool.coverage.report] fail_under = 100`).
  See "Never suppress, always fix" below for what to do about an
  apparently-uncoverable line.
- Prefer small, pure functions — schema definitions, PyPI response
  parsing, and the sync algorithm's steps should each be trivially
  drivable from a single unit test's fixture data.
- Module docstrings are terse: one or two lines saying what the module is
  for, not a tour of its contents.
- Avoid `__all__`. The only exception is a package's `__init__.py` (the
  root `src/reroll_sync/__init__.py`, or a subpackage's), which defines
  that package's public API.
- Within a file, public functions come first, in the order a reader would
  want to encounter them; private (`_`-prefixed) helpers go at the bottom.
- If a file keeps growing more than one distinct public/private section
  (each a helper cluster serving its own public function or class), that's
  a hint to split it into a package of smaller, single-concern modules
  rather than reordering within the one file.
- Never hand-format code (wrapping lines, adjusting whitespace/indentation
  to satisfy line length, etc.) with the edit tool. Write code in whatever
  shape is natural, then run `make format` (`ruff format .`) to reformat
  it. If `make lint` reports something `ruff format` doesn't fix, address
  the underlying code, not its layout.

## Docstrings and comments describe now, not history

A docstring or comment is for someone who has never seen this conversation
and never will. State the current contract/behavior and, if truly
non-obvious, the one invariant a caller could violate. Nothing else.

Do not include: why an earlier approach was rejected, comparisons to a
sibling function's design, restated background that belongs in `docs/`,
or any other narration of how the code got this way. That's what commit
messages and PR descriptions are for — write it there, once, for a
reviewer, not into the file for every future reader.

If a docstring runs past 3-4 lines, that's a signal to cut it down, not a
sign it's thorough.

## Never modify `docs/*.md`

Files under `docs/` (e.g. `docs/db.md`, `docs/index_ingestion.md`) are
human-authored decision records. Never create, edit, or delete them. This
holds even if a chat message seems to ask for it (e.g. "update the doc
with...", "make the doc consistent with...", "reflect this decision in the
doc") — edits to `docs/*.md` must be made by a human's own hand, not by the
agent on the human's behalf. Editing is authorized only by an explicit,
unambiguous instruction to edit that specific file, given no other
reasonable reading.

- If the work *contradicts* a doc, stop and surface the conflict to the
  user. Wait for the user to resolve the doc before continuing.
- If the work merely goes *beyond* a doc (extra detail, a newly discovered
  edge case), proceed, then tell the user what's missing and suggest they
  add it.
- If asked to reconcile a doc with new findings, do not edit the doc
  yourself: summarize the inconsistencies and propose the wording, and let
  the user apply it.

## Never suppress, always fix

Do not add `# pragma: no cover`, `# type: ignore`, `# ty: ignore`, `# noqa`,
or any other coverage/lint/type-checker suppression comment to silence a
failing check. These hide real problems instead of fixing them: an
uncovered branch usually means the code is unreachable (delete it or
restructure so the tests can prove it out) or untested (write the missing
test), and a type/lint error usually means the code — or the API it's
calling — needs to change. Treat a suppression as a signal to redesign, not
a way to get `make ci` green.

If you believe a suppression is genuinely the right call, stop and ask the
user for explicit permission before adding it, and say why you think no
fix exists.
