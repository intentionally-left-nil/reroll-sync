# 10 — Daemon, stage loops, control socket

**Depends on:** 08 (ingestion), 09 (fetch/convert), 06 (writer), 07
(dispatcher), 03 (limiter).

## Goal

`src/reroll_sync/daemon.py` and `src/reroll_sync/control.py`: the long-lived
process that runs every stage on its own schedule, plus the unix-socket
control plane that lets an admin operate it without fighting sqlite for the
write lock.

## Why a control socket rather than direct CLI writes

An admin will SSH in and run remediation commands while the daemon is
running. sqlite has one writer, and spec 06 gives that role to the daemon's
writer thread. Two processes both writing means `SQLITE_BUSY` and
unpredictable stalls.

So the split is:

- **Mutating commands** are thin clients over a unix domain socket. The
  daemon performs the write through its own writer and returns the result.
- **Read-only commands** open the database read-only (spec 01's
  `connect_reader`). WAL means they never block and are never blocked.

This is also what makes `reprocess` safe: it becomes one chunked set of
`WriteOp`s on the daemon's writer rather than an external process racing it.

## Requirements

### Configuration

One frozen dataclass, loaded from environment variables with defaults, no
hidden globals:

```
Config(
    db_path:            Path,
    segments_dir:       Path,
    socket_path:        Path,
    user_agent:         str,                 # required, no default
    global_rate:        float = 2000.0,      # per minute
    domain_reserves:    Mapping[str, float] = {"pypi.org": 200.0,
                                               "files.pythonhosted.org": 1800.0},
    fetch_workers:      int = 64,
    project_workers:    int = 32,
    convert_workers:    int = os.cpu_count() - 2,
    handoff_budget_bytes: int = 256 * 1024 * 1024,
    batch_size:         int = 1000,
    batch_interval:     float = 0.1,
    checkpoint_interval: float = 60.0,
    vacuum_interval:    float = 3600.0,
    index_poll_interval: float = 300.0,
    max_attempts:       int = 8,
    backoff_base:       float = 30.0,
    backoff_cap:        float = 21600.0,
    segment_seal_bytes: int = 64 * 1024 * 1024,
    segment_seal_seconds: float = 21600.0,
    disk_free_floor_bytes: int = 20 * 1024**3,
)
```

`user_agent` has no default so it cannot be forgotten — PyPI asks for a
descriptive one with contact info, and it is what keeps the service from
being blocked.

Validate at construction: rates positive, reserves summing to ≤ global,
worker counts ≥ 1, `convert_workers` ≥ 1 even on a 2-core box. Each
validation needs a test.

### Stage loops

Each stage is a supervised loop with its own trigger and its own
pause/resume state:

| Stage | Trigger | Notes |
|---|---|---|
| `index_poll` | every `index_poll_interval` | conditional GET; cheap on 304 |
| `project_sync` | queue non-empty | 32 threads, `pypi.org` reserve |
| `fetch` | queue non-empty | 64 threads, `files.` reserve |
| `convert` | queue non-empty | process pool; fed by fetch handoff and by bulk archive reads |
| `checkpoint` | every `checkpoint_interval` | owned by the writer (spec 06) |
| `vacuum` | every `vacuum_interval` | owned by the writer |
| `gc` | daily | `errors` retention only in Phase 1 |

A stage loop must:

- Exit promptly on a shutdown event; no unbounded blocking wait without a
  timeout.
- Catch and log any unexpected exception, then continue rather than killing
  the process — except for a configuration or programming error, which
  should crash loudly.
- Record last-run time, last-success time, and consecutive-failure count.
- Honour its pause flag: a paused stage claims no new work but lets in-flight
  work finish.

### Circuit breakers

Per external dependency, not per stage:

```
CircuitBreaker(failure_threshold=5, recovery_timeout=60.0, now=...)
```

- States: closed → open (after N consecutive failures) → half-open (after
  `recovery_timeout`, allows one trial) → closed or open again.
- Dependencies in Phase 1: `pypi.org`, `files.pythonhosted.org`, and the
  local disk (for segment writes).
- **An open breaker pauses only the stages that depend on it.** If
  `files.pythonhosted.org` is down, index polling and bulk convert from the
  archive must keep running. This isolation is the point of per-dependency
  breakers and needs an explicit test.
- `PyPIRateLimited` must **not** count toward the breaker. Throttling is
  expected behaviour, not a failure; conflating them would open the breaker
  during normal operation. Dedicated test.

### Disk guard

Before each segment append, and on a timer, check free space on
`segments_dir`. Below `disk_free_floor_bytes`, pause the fetch and archive
stages and log at error. This is what stops the service filling a shared
160 GB volume that also holds Docker images.

Resume automatically once space returns above the floor plus hysteresis
(e.g. floor × 1.2), so it does not flap.

### Startup sequence

Order matters:

1. Load and validate config.
2. `init_db` — fail fast on a schema or version mismatch.
3. Open the writer; recover `change_seq`.
4. **Archive recovery**: truncate any unsealed `.open` segment and reset the
   wheels whose blobs lived in it (spec 09).
5. Start the writer thread.
6. Start the control socket.
7. Start stage loops.

Steps 4 and 6 are both easy to get wrong by ordering: recovery must complete
before any stage can claim work, and the socket should be up early enough
that `status` works even if a stage fails to start.

### Shutdown

On `SIGTERM` / `SIGINT`:

1. Stop accepting new control requests except `status`.
2. Set the shutdown event; stages stop claiming.
3. Wait for in-flight work with a bounded grace period (e.g. 30 s).
4. Seal nothing — leave the open segment `.open`; spec 09's recovery handles
   it on next start. Sealing under time pressure risks a partial footer.
5. `writer.stop(drain=True)` — applies queued ops, final `TRUNCATE`
   checkpoint.
6. Remove the socket file.

A second signal during shutdown escalates to immediate exit.

### Control protocol

Keep it boring: newline-delimited JSON over `SOCK_STREAM`, one request per
connection, `{"command": str, "args": {...}}` in and
`{"ok": bool, "result": ..., "error": ...}` out.

Commands:

| Command | Effect |
|---|---|
| `status` | Full health snapshot (spec 11) |
| `pause <stage>` / `resume <stage>` | Toggle a stage |
| `drain` | Pause all claiming, finish in-flight, keep running |
| `reprocess <selector>` | Spec 07's campaign; returns affected count |
| `unquarantine <selector>` | Clear `work.quarantined_at`, requeue |
| `shutdown` | Graceful stop |

- **Permissions**: create the socket with mode `0600` and rely on filesystem
  permissions. No auth token in Phase 1 — the threat model is "other users on
  the box", which file mode covers. State this in the docstring so it is a
  decision rather than an oversight.
- Requests are handled on a small thread pool, and mutating commands go
  through `writer.submit_and_wait` so the reply reflects committed state.
- An unknown command returns `ok: false` with a message listing valid
  commands; it must not crash the daemon.
- A malformed request line, an oversized request, and a client that
  disconnects mid-write must all be handled without affecting other stages.

### Structured logging

- JSON lines to stdout (the supervisor captures them), with `wheel_id` /
  `filename` / `project` as correlation keys where applicable.
- One logger per stage under a `reroll_sync.` root, so an operator can tune
  volume per stage the way `reroll` itself does.
- reroll's own loggers (`reroll.scope`, `reroll.invalid`,
  `reroll.unconvertable`, `reroll.runtime`) will be extremely noisy at 12M
  wheels — every skipped wheel logs twice. **Set their levels explicitly at
  startup** (e.g. `reroll.scope` and `reroll.invalid` to `ERROR`) since
  reroll-sync records skips in the database anyway. Without this the log
  volume alone could be a problem.

## Tests to write first

**Config**

- Missing `user_agent` raises.
- Reserves summing above the global rate raise.
- A non-positive rate, a zero worker count, and a negative budget each raise.
- `convert_workers` is ≥ 1 on a hypothetical 1-core machine.

**Stage loops** — all with injected clocks and fake stages; no sleeps.

- A loop runs on its interval and not more often.
- A paused stage claims nothing but completes in-flight work.
- `resume` restores claiming.
- An exception in one iteration is logged and the loop continues.
- A shutdown event stops the loop within one iteration.
- Last-run / last-success / consecutive-failure counters update correctly.

**Circuit breakers**

- 5 consecutive failures open the breaker.
- An open breaker rejects immediately without calling the dependency.
- After `recovery_timeout` the breaker allows exactly one trial.
- A successful trial closes it; a failed trial reopens it.
- A success before the threshold resets the count.
- `PyPIRateLimited` does not count toward the threshold.
- An open `files.pythonhosted.org` breaker leaves `index_poll` and bulk
  convert running. **Key isolation test.**

**Disk guard**

- Below the floor, fetch and archive pause and an error is logged.
- Above floor × 1.2, they resume.
- Between floor and floor × 1.2, state does not flap.

**Startup / shutdown**

- A schema mismatch aborts startup before any stage starts.
- Archive recovery runs before any stage claims work. Assert ordering.
- The socket answers `status` even when a stage failed to start.
- `SIGTERM` drains the writer and applies queued ops.
- `SIGTERM` leaves the open segment as `.open` and does not seal it.
- The socket file is removed on exit.
- A stale socket file from a previous crash is replaced, not fatal.
- A second signal during shutdown exits promptly.

**Control protocol**

- Every command round trips over a real socket in a test.
- `pause`/`resume` observably change stage behaviour.
- `reprocess` returns the affected count and the effect is committed before
  the reply.
- An unknown command returns an error listing valid commands.
- Malformed JSON, an oversized request, and a mid-write disconnect are each
  handled without affecting the daemon.
- Concurrent clients are served.
- The socket is created mode `0600`.

## Acceptance criteria

- The daemon runs indefinitely with all stages, and a `SIGTERM` leaves the
  database consistent (verified by `fsck` in a test).
- No stage can starve another: index polling continues during a saturated
  metadata backfill, proven by test using the limiter.
- A dependency outage pauses only its dependents.
- Every configuration value is injectable; nothing is read from the
  environment outside `Config`.
- No test sleeps.
- `make ci` green, coverage 100%.

## Deferred

- systemd unit files and deployment packaging.
- Auth on the control socket beyond file mode.
- Hot config reload.
- A publish stage (Phase 2).
