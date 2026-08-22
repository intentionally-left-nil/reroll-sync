# 03 — Hierarchical rate limiter

**Depends on:** nothing (pure, no DB). Can be built in parallel with 01/02.

## Goal

`src/reroll_sync/ratelimit.py`: a thread-safe token bucket hierarchy that
caps all PyPI traffic at 2,000 requests/minute while guaranteeing index
polling can never be starved by a multi-day metadata backfill.

## Why hierarchical rather than one bucket

A cold start is ~5.5 hours of project pages followed by up to ~3.5 days of
`.metadata` fetches. If both draw from one FIFO budget, the backfill
consumes the entire budget for days and the service is blind to everything
PyPI publishes in the meantime.

Splitting by domain fixes this **structurally rather than by policy**:

```
global: 2000/min
  ├── pypi.org                 reserve  200/min   (index poll, project pages)
  └── files.pythonhosted.org   reserve 1800/min   (.metadata sidecars)
```

A child may borrow the other's idle capacity, but never below the other's
reserve, and never above the global cap. So metadata backfill uses ~1,990/min
when the index is caught up, and drops to 1,800/min the moment index work
appears — automatically, with no mode detection.

## Requirements

### `TokenBucket`

```
TokenBucket(rate_per_minute: float, burst: float, now: Callable[[], float])
```

- Lazy refill: compute tokens from elapsed monotonic time on each call, do
  not run a timer thread.
- `try_acquire(n=1) -> bool` — non-blocking.
- `acquire(n=1, timeout=None) -> bool` — blocks using a `threading.Condition`
  with a computed wait, never a busy loop. Returns `False` on timeout.
- `available()` — current token count, for metrics.
- `drain()` — set tokens to zero. Used for `Retry-After` handling.
- `penalize(seconds)` — refuse all acquisitions until a deadline. This is
  the 429 response: a `Retry-After: 60` must stop that domain for 60
  seconds, not merely zero its tokens (which would refill in a second).

`burst` should default to roughly one second of rate, so ~33 for the global
bucket. A large burst defeats the purpose; a burst below the concurrency of
the calling pool causes needless serialization.

### `HierarchicalLimiter`

```
HierarchicalLimiter(
    global_rate_per_minute: float,
    children: Mapping[str, float],   # name -> reserved rate/min
    now: Callable[[], float] = time.monotonic,
)
```

- `acquire(child_name, n=1, timeout=None) -> bool` — must take from **both**
  the child bucket and the global bucket, and must be atomic: if the global
  bucket cannot satisfy the request, tokens already taken from the child are
  returned. A partial acquisition that silently overdraws the global cap is
  the bug this class exists to prevent, and it needs its own test.
- Borrowing: a child whose own reserve is exhausted may still acquire if the
  global bucket has tokens **and** every other child is at or above its
  reserve. Concretely — a child's effective ceiling is
  `min(global_available, its_own_available + unclaimed_global_slack)`.
  Implement whichever formulation you can state precisely and test; the
  required *observable* behaviours are:
  - With only one child active, it approaches the global rate.
  - With both active and both saturated, each gets at least its reserve.
  - The sum across children never exceeds the global rate over any window.
- `penalize(child_name, seconds)` affects that child only. A 429 from
  `files.pythonhosted.org` must not stop index polling.
- `snapshot()` — per-child and global available tokens, plus per-child
  cumulative acquired/denied counters and current penalty deadline. Spec 11
  reports these.
- The default `children` mapping for production lives in config (spec 10),
  not hard-coded here: `{"pypi.org": 200, "files.pythonhosted.org": 1800}`.

### Threading

One `threading.Lock` guarding the whole hierarchy is correct and fast enough
— acquisitions happen ~33 times/second, so contention is irrelevant and a
single lock removes any possibility of a lock-ordering bug between the child
and global buckets. Do not build per-bucket locking.

Blocking `acquire` must use `Condition.wait(timeout)` with the timeout
computed from the deficit and the refill rate, then re-check. It must wake
promptly when another thread returns tokens (the atomicity rollback above).

## Tests to write first

Every test injects a fake clock. **No test may call `time.sleep`.**

**`TokenBucket`**

- A fresh bucket permits `burst` acquisitions then refuses.
- After advancing the clock by `60/rate` seconds, exactly one more token is
  available.
- Tokens never exceed `burst` no matter how far the clock advances.
- `try_acquire(n)` for `n > burst` always returns `False` rather than
  blocking forever or deadlocking.
- `acquire(timeout=0)` on an empty bucket returns `False` immediately.
- `acquire(timeout=t)` succeeds once the clock advances enough — drive by
  advancing the fake clock from a second thread, or by making the fake clock
  auto-advance on `wait`.
- `drain()` empties the bucket; the next refill tick still works.
- `penalize(s)` refuses even a full bucket until the deadline passes.
- `penalize` while already penalized extends but never shortens the
  deadline.
- Fractional rates (e.g. 0.5/min) do not divide by zero or refill in
  negative amounts.
- A non-monotonic clock going backwards does not create tokens.

**`HierarchicalLimiter`**

- Acquiring from a child decrements both that child and the global bucket.
- **Atomic rollback**: with the global bucket empty and a child bucket full,
  `acquire` returns `False` *and* the child's token count is unchanged.
- With one child idle, the active child's sustained rate approaches the
  global rate (advance the clock over a simulated window and count grants).
- With both children saturated, each receives at least its reserve over a
  simulated window.
- Over any simulated window, total grants across children never exceed the
  global rate. Assert this with a loop over a long simulated period —
  this is the single most important test in the file.
- `penalize("files.pythonhosted.org", 60)` blocks that child while
  `pypi.org` continues to be served.
- An unknown child name raises `KeyError` rather than silently bypassing the
  limit.
- `snapshot()` counters reflect grants and denials.
- Concurrency smoke test: N threads each acquiring in a loop against a
  known budget over a fixed simulated window never exceed the global cap.
  Use a real `threading` test with a deterministic fake clock advanced by
  the test, not by the workers.

## Acceptance criteria

- Sum of grants across all children provably never exceeds the global rate,
  demonstrated by test over a simulated window of at least 10 minutes.
- Zero `time.sleep` in `src/` and in the tests for this module.
- `snapshot()` supplies everything spec 11 needs for the
  `bucket_utilization` metric without further plumbing.
- `make ci` green, coverage 100%.

## Deferred

- Adaptive rate discovery (raising the cap if PyPI tolerates more). The
  2,000/min figure is a deliberate self-imposed limit.
- Per-host DNS-level sharding, connection-count limits. Spec 04 handles
  connection pooling.

## Note

The `2000/min` global figure and the `200/1800` split are configuration, not
constants of nature. Put them in the config object from spec 10 with these
as defaults, and make sure nothing in `ratelimit.py` hard-codes a domain
name.
