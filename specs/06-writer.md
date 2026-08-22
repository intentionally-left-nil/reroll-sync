# 06 — The sqlite writer thread

**Depends on:** 01.

## Goal

`src/reroll_sync/writer.py`: the single component permitted to write to
sqlite at runtime. It batches transactions, keeps the WAL flat, and provides
the read-transaction watchdog everything else is measured against.

## Why a dedicated writer

- **sqlite has one writer.** Multiple threads writing means `SQLITE_BUSY`
  retries and unpredictable latency. Funnelling through one thread removes
  the class of problem entirely.
- **Per-row `commit()` is the current code's worst performance bug.** Every
  one of `metadata_sync.py`, `metadata_parse.py`, and `reroll_convert.py`
  commits after each wheel. During a bulk re-convert at thousands of wheels
  per second that caps throughput at a few hundred per second.
- **WAL growth is a correctness-adjacent risk**, not just disk usage. A
  checkpoint cannot advance past the oldest active reader, so a single
  leaked long reader makes the WAL grow without bound until the disk fills.
  Somebody has to own detecting that; it is this module.

## Requirements

### Interface

```
Writer(
    conn: sqlite3.Connection,           # from connect_writer()
    *,
    batch_size: int = 1000,
    batch_interval: float = 0.1,        # seconds
    checkpoint_interval: float = 60.0,
    now: Callable[[], float] = time.monotonic,
)
```

- `submit(op: WriteOp) -> None` — enqueue onto a **bounded** `queue.Queue`.
  Bounded so that a stalled writer applies backpressure to producers instead
  of growing an unbounded in-memory backlog. When full, `submit` blocks.
- `submit_and_wait(op) -> Any` — for the control plane (spec 10), where an
  admin command must see its effect before the CLI returns. Implement with a
  per-op `threading.Event`.
- `start()` / `stop(drain: bool = True)`. `stop` with `drain=True` applies
  everything queued, commits, runs a final `TRUNCATE` checkpoint, and closes.
- Runs as one daemon thread.

### `WriteOp`

A `WriteOp` is a callable-plus-description, not raw SQL strings scattered
around the codebase:

```
WriteOp(
    name: str,                                     # for metrics and logs
    apply: Callable[[sqlite3.Connection], Any],    # runs inside the batch txn
    result_event: threading.Event | None = None,
)
```

Each stage defines its own ops (spec 07 defines the pipeline ones, spec 08
the ingestion ones). `apply` must never call `commit()`, `rollback()`, or
`BEGIN` — the writer owns transaction boundaries. Enforce with a test that a
`WriteOp` calling `commit()` is detected and raises.

### Batching

- Accumulate ops until `batch_size` ops **or** `batch_interval` elapsed,
  whichever first, then one `COMMIT`.
- An exception from one `apply` must not poison the batch. Options: roll back
  the whole batch and re-apply ops individually to isolate the offender, or
  wrap each `apply` in a `SAVEPOINT`. **Choose savepoints** — re-applying is
  not safe for ops that are not idempotent. The failing op's exception is
  recorded and surfaced; siblings still commit.
- Failed ops increment a `writer_failed_ops` counter and log at error with
  the op `name`. A repeatedly failing op is a bug, not something to retry
  silently.

### WAL management

- Every `checkpoint_interval`, between batches (never mid-batch), run
  `PRAGMA wal_checkpoint(TRUNCATE)`.
- Record `last_truncate_checkpoint_at` and whether it succeeded. `TRUNCATE`
  returns a busy indicator when it cannot complete; treat that as a failure
  and **do not** silently ignore it.
- Track consecutive failures. After N (say 5), log at error naming the
  likely cause — a leaked read transaction — and expose it in health
  (spec 11). This is the single highest-signal alarm in the system.
- Expose current WAL size in bytes (stat the `-wal` file).

`wal_autocheckpoint = 1000` from spec 01 handles ordinary passive
checkpointing; it reuses the WAL file but **never shrinks it**. `TRUNCATE` is
the only thing that resets the file size, which is why it is scheduled
explicitly here.

### Incremental vacuum

- Every `vacuum_interval` (default: 1 hour), run
  `PRAGMA incremental_vacuum(N)` with a bounded page count (e.g. 10,000) so
  it never becomes a long operation.
- Track and expose `freelist_count` so a growing freelist is visible.

Spec 01 sets `auto_vacuum = INCREMENTAL`; this is where it is actually
exercised. Bulk reprocess campaigns delete millions of `wheel_repodata` rows
and this is what returns that space.

### The read-transaction watchdog

Also lives here because it is inseparable from WAL health.

```
@contextmanager
def read_txn(conn, *, budget: float = 0.25, label: str): ...
```

- Times the block. On exceeding `budget`, logs at warning with the label and
  duration, and increments a counter.
- Records the observed maximum for health reporting
  (`longest_read_txn_ms`).
- **Every** read path in the codebase — dispatcher queue queries, ingestion's
  serial map, health counts, `fsck`, CLI — uses this wrapper. Grep-able
  compliance: no bare `conn.execute` for a multi-row read outside this
  helper.
- Optionally support a strict mode that raises instead of warning, enabled in
  tests, so an accidental unbounded read fails CI rather than merely logging.

### The monotonic `change_seq` counter

`wheels.change_seq` needs a source. The writer owns it:

- On start, `SELECT COALESCE(MAX(change_seq), 0) FROM wheels`.
- Hand out increasing values in-process. Since the writer is the only writer,
  no coordination is needed.
- Expose `current_seq()`.

Phase 2 uses `ix_wheels_change_seq` to answer "what changed since seq N"
without needing a `dirty_packages` table. Every op that mutates a `wheels`
row must set `change_seq` to a fresh value — enforce it in `fsck` (spec 11)
by checking for duplicate seq values across different `updated_at`.

## Tests to write first

All tests use an injected clock and a real in-memory or `tmp_path` sqlite
database. **No test may sleep.** Where a background thread is involved, drive
it with explicit events, not timing.

**Batching**

- `batch_size` ops produce exactly one `COMMIT` (count via
  `sqlite3.Connection.set_trace_callback` or a counting wrapper).
- Fewer than `batch_size` ops still commit once `batch_interval` elapses on
  the injected clock.
- 2,500 ops with `batch_size=1000` produce exactly 3 commits.
- Ops apply in submission order.
- `submit_and_wait` returns the `apply` return value.
- `submit_and_wait` propagates an exception raised by `apply`.

**Failure isolation**

- One failing op among ten: the other nine are committed and visible.
- The failing op's exception is recorded with its `name`.
- `writer_failed_ops` increments by exactly one.
- A `WriteOp` whose `apply` calls `conn.commit()` is rejected/raises.

**Bounded queue and backpressure**

- With the writer stopped and the queue at capacity, `submit` blocks rather
  than growing.
- After `stop(drain=True)`, every queued op has been applied.
- After `stop(drain=False)`, queued ops are discarded and the method still
  returns.
- `submit` after `stop` raises rather than silently dropping.

**WAL**

- After enough writes to grow the WAL, a `TRUNCATE` checkpoint reduces the
  `-wal` file size. Assert on the actual file size — this is the regression
  test that matters.
- With a read transaction deliberately held open on a second connection,
  `TRUNCATE` fails, the failure is counted, and `last_truncate_checkpoint_at`
  does **not** advance.
- After that reader closes, the next attempt succeeds and the counter resets.
- 5 consecutive failures produce the error-level log naming a leaked reader.
- `wal_bytes` reflects the real file size.

**Incremental vacuum**

- Deleting many rows raises `freelist_count`; a vacuum pass lowers it.
- `incremental_vacuum` runs with a bounded page count and does not block a
  concurrent reader for longer than the read budget.

**Watchdog**

- A read inside budget logs nothing and increments nothing.
- A read exceeding budget (injected clock) logs once with the label.
- `longest_read_txn_ms` records the maximum across several reads.
- Strict mode raises on exceeding budget.

**`change_seq`**

- A fresh database starts at 1.
- Restarting the writer against an existing database resumes above the
  stored maximum.
- Concurrent producers submitting ops receive strictly increasing seq values
  with no duplicates.

## Acceptance criteria

- No module in `src/` outside `writer.py` calls `conn.commit()`,
  `conn.rollback()`, or executes `BEGIN`. Enforce with a test that greps the
  source tree, or by review checklist — prefer the test.
- No module in `src/` performs a multi-row read outside `read_txn`.
- A `TRUNCATE` checkpoint provably shrinks the WAL file in a test.
- A held reader provably prevents it, and that condition is observable in
  health output.
- `make ci` green, coverage 100%.

## Deferred

- Multi-process writing. Offline tools (spec 13's import) write with the
  daemon stopped; that is the whole concurrency story for Phase 1.
- Write-ahead batching by table or op type. One FIFO is sufficient at these
  rates.

## Note on `synchronous = NORMAL`

With `NORMAL`, a `COMMIT` does not fsync the WAL, so an OS crash can lose the
last batches. That is deliberate (spec 01): every row is re-derivable from
PyPI or the segment store, and the throughput difference over 12M rows is
large. Do not "fix" it to `FULL`.
