# 12 — CLI

**Depends on:** 10 (daemon/control), 11 (health/fsck), 02 (verify-archive),
07 (reprocess).

## Goal

Rewrite `cli.py`. The CLI becomes two things: a launcher for the daemon, and
a client for operating it. The four batch subcommands that exist today go
away, because their work now happens continuously inside the daemon.

## Why the current CLI has to change

`cli.py` today exposes `init`, `sync-index`, `sync-metadata`,
`parse-metadata`, `sync-reroll`, and `stats`. Each opens its own writable
connection, does a bounded batch, prints a one-line summary, and exits. That
model is incompatible with the new design in three ways:

1. **Two writers.** Any of those commands run against a live daemon would
   contend for the sqlite write lock.
2. **The stages no longer exist as separate passes.** `sync-metadata` and
   `parse-metadata` are fused (spec 09); `wheel_metadata` is gone entirely.
3. **`main()` calls `init_db` on every invocation**, including for read-only
   commands, which would now mean a writable open just to run `stats`.

## Requirements

### Command structure

```
reroll-sync init [--db PATH]
reroll-sync run  [--config-from-env]              # the daemon, foreground

# read-only: direct read-only DB access, safe with the daemon running
reroll-sync status [--json]
reroll-sync errors [--category C] [--since D] [--limit N]
reroll-sync fsck [--chunk N]
reroll-sync verify-archive [--segment ID]
reroll-sync queue [--stage S]

# mutating: over the control socket, requires a running daemon
reroll-sync pause <stage>
reroll-sync resume <stage>
reroll-sync drain
reroll-sync reprocess [--reroll-version-below V | --state S | --project P
                       | --skipped-only] [--dry-run]
reroll-sync unquarantine [--stage S | --project P | --all]
reroll-sync shutdown
```

### Three command classes, three behaviours

This distinction must be structural in the code — a table or registry mapping
each command to its class — not ad-hoc `if` branches:

| Class | DB access | Requires daemon | On missing daemon |
|---|---|---|---|
| `init` | writable, exclusive | must **not** be running | error clearly |
| read-only | `connect_reader` | no | works fine |
| mutating | none directly | yes | clear error naming the socket path |

**Read-only commands must never call `init_db`.** Today `main()` calls it
unconditionally; that must go. A read-only command against a nonexistent
database should say so plainly, not create an empty one.

**`init` must refuse to run while the daemon holds the socket**, since it
validates and potentially creates schema. Check for a live socket and error
out.

### `reprocess --dry-run`

Required, not optional. A reprocess campaign can touch 12M rows. `--dry-run`
reports the affected count and, crucially, **which `skips` rows would be
cleared**, without writing. An operator should be able to see that a
`--reroll-version-below` will requeue 400k previously-skipped wheels before
committing to it.

### `status` output

Human-readable by default, grouped and short enough to read in a terminal —
lag and alarms first, because those are what the operator came for:

```
reroll-sync: running (uptime 4d 02:11)

ALARMS
  ! wal 2.4 GB, 3 failed truncate checkpoints  (likely leaked reader)
  ~ 412 wheels quarantined

FRESHNESS       index lag 1,204 serials   last poll 41s ago
QUEUES          fetch 8,441,203 (lane1)   convert 12,904 (lane0)
STATES          ready 3,102,884  need_metadata 8,441,203  skipped 88,201
                no_metadata 402,113  quarantined 412  deleted 1,208
                requires_prerelease 24,881
RATE            pypi.org 12/200/min   files.pythonhosted.org 1,794/1800/min
ARCHIVE         41 sealed (5.9 GB)  open 22 MB, age 1h14m
SQLITE          db 12.8 GB  wal 2.4 GB  longest read 180ms
```

`--json` emits the full `Health` dataclass for scripting. Both come from the
same `snapshot()`; no separate formatting path may compute a number itself.

### Exit codes

Meaningful, so these work in cron:

| Code | Meaning |
|---|---|
| 0 | success, no problems |
| 1 | command error (bad args, missing DB, no daemon) |
| 2 | `fsck` / `verify-archive` found violations |
| 3 | `status` found a critical alarm |

### Argument parsing

Keep `argparse` and the existing `build_parser()` shape — it is testable and
adequate. Improvements needed:

- `db_path` becomes a `--db` option with an env-var default
  (`REROLL_SYNC_DB`), not a positional. It is now the same for every command
  and a positional invites mistakes like `reroll-sync pause fetch` being
  parsed as a db path.
- Every subcommand gets `--help` text stating whether it needs a running
  daemon.
- Dispatch via a registry mapping name → (class, handler), so adding a
  command cannot forget to declare its class.

The current `main()` is a chain of `if args.command == ...` blocks with a
fall-through default for `sync-index`. Replace it; a fall-through default is
how a typo becomes a surprise index sync.

## Tests to write first

**Structure**

- Every command is reachable and `--help` works for each.
- Each command's class is declared in the registry; a test iterates the
  registry and asserts every parser subcommand has an entry (catches drift).
- No fall-through default: an unknown command errors.

**Read-only commands**

- `status`, `errors`, `fsck`, `verify-archive`, `queue` never open a writable
  connection. Assert by pointing them at a read-only file, or by injecting a
  factory that fails on writable opens.
- None of them calls `init_db`.
- Each against a nonexistent database exits 1 with a clear message and does
  **not** create the file.
- Each works while a simulated daemon holds a write connection.

**`init`**

- Creates the database and exits 0.
- Idempotent on a second run.
- Exits 1 on a schema mismatch, printing the specific problems (the existing
  `SchemaMismatchError` message shape is good — keep it).
- Exits 1 if a live control socket is present.

**Mutating commands**

- Each sends the right command and args over the socket.
- With no daemon, each exits 1 naming the socket path.
- With a socket present but not accepting, each exits 1 without hanging
  forever (bounded connect timeout).
- The daemon's error reply is surfaced verbatim.

**`reprocess`**

- `--dry-run` writes nothing and reports both the wheel count and the
  `skips` count that would be cleared.
- Without `--dry-run`, the campaign is submitted and the count returned.
- Mutually exclusive selectors are rejected by the parser.
- No selector at all is rejected — a bare `reprocess` must not mean
  "everything".

**Output**

- `status --json` round trips through `json.loads` and contains every
  `Health` field.
- Human output includes lag, every non-zero state count, and all alarms.
- Human and JSON output derive from the same snapshot (inject a snapshot and
  assert both render its values).
- Exit code 3 when a critical alarm is present, 0 when not.
- Exit code 2 when `fsck` finds a violation, 0 when clean.
- Byte counts are formatted human-readably and the formatter is tested at
  0, 1023, 1024, and a multi-TB value.

## Acceptance criteria

- `sync-index`, `sync-metadata`, `parse-metadata`, and `sync-reroll` no
  longer exist as CLI commands.
- No read-only command opens a writable connection or calls `init_db`.
- Every mutating command goes over the control socket; none writes directly.
- `reprocess` has a working `--dry-run` that reports skip clearing.
- Exit codes are as tabled and tested.
- `make ci` green, coverage 100%.

## Deferred

- Shell completion.
- A TUI or watch mode. `status` plus `watch` is enough.
- `import-metadata` as a subcommand — spec 13 keeps the bulk import as an
  external script deliberately.
