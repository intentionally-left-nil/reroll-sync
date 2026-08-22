# 04 — PyPI client

**Depends on:** 01 (column set), 03 (rate limiter).

## Goal

Rewrite `pypi_client.py` as a connection-pooling HTTP client over `httpx`
that handles conditional GETs, 429/`Retry-After`, and parses simple-index
responses into the exact typed shape `wheels` stores. Delete
`metadata_download.py` and fold its one function in here.

## Why the current client has to be replaced

- `urllib.request.urlopen` opens a **fresh TCP+TLS connection per request**.
  At 33 req/s sustained for days that is enormous waste and pointless
  latency. `httpx` gives keep-alive and HTTP/2 with a pooled client.
- No conditional GET. `/simple/` is ~25 MB; polling it every minute
  unconditionally is ~36 GB/day of downloads to learn nothing.
- No 429 handling at all, so the rate limiter has no feedback path.
- `fetch_project` and `fetch_metadata` raise raw `OSError`/`HTTPError`, which
  callers currently swallow with `except OSError: continue`. The dispatcher
  needs to distinguish "retry soon", "retry much later", and "this URL is
  permanently gone".

## Requirements

### Client object, not module functions

```
PyPIClient(
    limiter: HierarchicalLimiter,
    *,
    index_url: str = "https://pypi.org/simple/",
    timeout: float = 30.0,
    user_agent: str,
    transport: httpx.BaseTransport | None = None,   # test seam
)
```

- Holds one `httpx.Client` with `http2=True` and explicit pool limits sized
  to the fetch concurrency (spec 09), e.g.
  `max_connections=100, max_keepalive_connections=100`.
- Context manager; closes the pool.
- **Sets a descriptive `User-Agent` including a contact URL.** PyPI asks for
  this and it is what stops the service being blocked. Make it required, not
  defaulted, so it cannot be forgotten.
- Every request acquires from the limiter first, keyed by the request's
  host. Derive the child key from the URL host so a redirect to an
  unexpected host cannot bypass the limiter — an unknown host must raise
  (spec 03 makes `KeyError` the behaviour).

### Typed results

```
IndexProject(name: str, serial: int)

SimpleIndex(
    last_serial: int,
    projects: tuple[IndexProject, ...],
    etag: str | None,
    not_modified: bool,
)

ProjectFile(
    filename: str,
    url: str,
    wheel_sha256: str | None,
    metadata_sha256: str | None,   # None => no PEP 658 sidecar at all
    has_metadata: bool,            # True with metadata_sha256 None => unhashed
    size: int | None,
    upload_time: str | None,
    requires_python: str | None,
    yanked: bool,
    yanked_reason: str | None,
)

ProjectPage(last_serial: int, files: tuple[ProjectFile, ...])
```

`ProjectFile` must map 1:1 onto the normalized `wheels` columns from spec 01.
**Do not retain the raw JSON object** — that is exactly the 6–12 GB the new
schema eliminates.

### `metadata_hashes` logic must be preserved

The existing `metadata_hashes()` in `pypi_client.py` correctly handles the
PEP 714 / PEP 658 mess: `core-metadata` and `dist-info-metadata` may each be
`False`, `True`, or `{"sha256": ...}`, an object from either field wins over
a bare `True` from either. **Port this function and its semantics exactly**,
then express the result as the `metadata_sha256` / `has_metadata` pair
above. Its existing tests in `tests/test_pypi_client.py` are good — carry
them over.

Mapping:

| `metadata_hashes` result | `has_metadata` | `metadata_sha256` |
|---|---|---|
| `None` | `False` | `None` |
| `{}` | `True` | `None` |
| `{"sha256": "ab..."}` | `True` | `"ab..."` |
| `{"md5": "..."}` (no sha256) | `True` | `None` |

### `yanked` parsing

The simple API's `yanked` is `false`, `true`, or a **string reason**. A
non-empty string means yanked *with* that reason. Parse into
`(yanked: bool, yanked_reason: str | None)`. The empty string is yanked with
no reason. This needs its own tests — it is a classic mis-parse.

### Conditional GET on `/simple/`

- `fetch_simple_index(etag: str | None) -> SimpleIndex` sends
  `If-None-Match` when given an etag.
- A `304` returns `SimpleIndex(not_modified=True, projects=(), ...)` with
  the etag echoed, and must **not** raise.
- The caller (spec 08) persists the etag. Store it in a small
  `daemon_state(key, value)` key/value table, or in the daemon's on-disk
  state file — spec 08 decides; this spec only requires the client accept
  and return it.
- Also send `If-Modified-Since` if a `Last-Modified` was seen. Harmless if
  PyPI ignores it.

### Error taxonomy

Raise these, all subclassing `PyPIError`. This is the contract the
dispatcher's backoff logic depends on, so it must be explicit rather than
leaking `httpx` exceptions:

| Exception | Raised for | Dispatcher meaning |
|---|---|---|
| `PyPITransientError` | connect/read timeout, 5xx, connection reset | Retry with backoff |
| `PyPIRateLimited(retry_after: float \| None)` | 429, and 403 with a rate-limit body | `limiter.penalize()`, retry |
| `PyPINotFound` | 404, 410 | Terminal for this URL — project or file is gone |
| `PyPIProtocolError` | non-JSON body, missing `meta`/`files`, bad `Accept` handling | Retry with backoff, alarm loudly |

- Parse `Retry-After` in both delta-seconds and HTTP-date forms. Both need
  tests.
- Never retry internally. Retries are the dispatcher's job (spec 07) so that
  attempts, backoff, and quarantine are all recorded in one place. A hidden
  retry loop inside the client would make `work.attempts` meaningless.

### `fetch_metadata`

```
fetch_metadata(url: str, expected_sha256: str | None) -> bytes
```

- Appends nothing — the caller passes the full `.metadata` URL. (Today
  `metadata_sync.py` does `f"{wheel.url}.metadata"`; keep that construction
  at the call site in spec 09 where the wheel row is in hand.)
- Verifies sha256 when `expected_sha256` is given and raises
  `MetadataHashMismatch(expected, actual)` — a distinct exception, because
  it is neither transient nor a protocol error: it means PyPI served
  something inconsistent with its own index, which is a recordable data
  error, not a retry.
- Enforces a maximum response size (e.g. 32 MB) and raises
  `PyPIProtocolError` beyond it, so a pathological response cannot exhaust
  the bounded in-memory handoff queue.

### `Accept` header

Send `application/vnd.pypi.simple.v1+json` on index and project requests. If
the response `Content-Type` is not the JSON simple API, raise
`PyPIProtocolError` rather than attempting to parse HTML. Today the code
sends the header but never checks the response type.

## Tests to write first

Use `httpx.MockTransport` — it is a first-class test seam and needs no
network. No test may make a real request.

**Parsing**

- Index response parses `meta._last-serial` and every project's
  `_last-serial`.
- Project page parses each documented `ProjectFile` field.
- All four `metadata_hashes` cases from the table above, plus the
  object-beats-`True` precedence in both field orders (carry over the
  existing tests).
- `yanked: false` / `true` / `"reason text"` / `""` all parse correctly.
- Missing optional fields (`size`, `upload-time`, `requires-python`) yield
  `None`, not `KeyError`.
- A file entry with no `hashes` key at all.
- Non-`.whl` filenames are returned by the client unchanged — filtering is
  the ingestion stage's job (spec 08), not the client's.
- A project page with an empty `files` list.
- Unicode in a project name is percent-encoded correctly in the URL.
- A project name needing PEP 503 normalization is requested at its
  normalized path.

**Conditional GET**

- With no etag, no `If-None-Match` is sent.
- With an etag, `If-None-Match` is sent verbatim.
- A `304` yields `not_modified=True`, empty `projects`, no exception.
- A `200` yields the new etag from the response header.

**Errors**

- `500`, `502`, `503` → `PyPITransientError`.
- Connect timeout and read timeout → `PyPITransientError`.
- `404` and `410` → `PyPINotFound`.
- `429` with `Retry-After: 30` → `PyPIRateLimited(retry_after=30)`.
- `429` with an HTTP-date `Retry-After` → correct seconds computed against
  an injected clock.
- `429` with no `Retry-After` → `retry_after=None`.
- `200` with `Content-Type: text/html` → `PyPIProtocolError`.
- `200` with truncated/invalid JSON → `PyPIProtocolError`.
- `200` missing `meta` → `PyPIProtocolError`.
- The client never retries internally: a transport that fails once then
  succeeds results in exactly one attempt and a raised exception.

**Rate limiting integration**

- Every request acquires exactly one token from the limiter child matching
  the URL host.
- A request to an unexpected host raises rather than proceeding
  unthrottled — e.g. a redirect to `example.com`.
- When `limiter.acquire` returns `False` (timeout), the client raises
  `PyPIRateLimited` rather than making the request.

**`fetch_metadata`**

- Correct bytes returned and sha256 verified.
- A mismatching sha256 raises `MetadataHashMismatch` carrying both digests.
- `expected_sha256=None` skips verification.
- A response exceeding the size cap raises `PyPIProtocolError`.

## Acceptance criteria

- `metadata_download.py` is deleted; nothing imports it.
- No `urllib` import remains in `src/`.
- One pooled `httpx.Client` per `PyPIClient`; no per-request client
  construction anywhere.
- Every exception the client raises is a `PyPIError` subclass. No `httpx`
  exception escapes.
- `User-Agent` is a required constructor argument.
- `make ci` green, coverage 100%.

## Deferred

- Mirror / alternate index support. `index_url` is parameterized, but only
  `pypi.org` is exercised.
- HTTP caching beyond the index etag. Per-project-page etags would be a
  meaningful future optimization (650k conditional GETs are much cheaper
  than 650k full pages) but the serial check already avoids most fetches.
