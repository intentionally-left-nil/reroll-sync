# reroll-sync

Generates repodata, synced from pypi `reroll-sync` is two things in one binary: a long-lived **daemon** that
does the syncing, and a **CLI** that operates it (checks health, pauses
stages, requeues work, etc.).

# Installation

Requires Python 3.13+. Install straight from this repo with
[uv](https://docs.astral.sh/uv/):

```sh
uv tool install --reinstall git+https://github.com/intentionally-left-nil/reroll-sync
```

## Quickstart

Set the appropriate environment variables (see [Environment Variables](#environment-variables))

```sh
# 1. Create the sqlite database. Refuses if a daemon is already running.
reroll-sync init
# 2. Run the daemon in the foreground (use a process supervisor in
#    production -- there is no built-in daemonization).
reroll-sync run --config-from-env
```

In another terminal, once it's running:

```sh
reroll-sync status
```

## Commands

```
reroll-sync init  [--db PATH] [--socket PATH]
reroll-sync run   [--config-from-env]
```

Read-only -- safe to run at any time, whether or not the daemon is up:

```
reroll-sync status [--json]
reroll-sync errors [--category C] [--since ISO8601] [--limit N]
reroll-sync fsck [--chunk N]
reroll-sync verify-archive [--segment ID]
reroll-sync queue [--stage fetch|convert]
```

Daemon manipulation commands
```
reroll-sync pause  <fetch|convert>
reroll-sync resume <fetch|convert>
reroll-sync drain
reroll-sync reprocess (--reroll-version-below V | --state STATE | --project P | --skipped-only) [--dry-run]
reroll-sync unquarantine (--stage STATE | --project P | --all)
reroll-sync shutdown
```

# Environment Variables

| Env var | Default | Meaning |
|---|---|---|
| `REROLL_SYNC_USER_AGENT` | HTTP User-Agent sent to PyPI | *(required, no default)* |
| `REROLL_SYNC_DB_PATH` | sqlite database path | `reroll_sync.db` |
| `REROLL_SYNC_SEGMENTS_DIR` | archive directory | `segments` |
| `REROLL_SYNC_SOCKET_PATH` | control socket path | `reroll_sync.sock` |
| `REROLL_SYNC_GLOBAL_RATE` | `2000.0` | total PyPI requests/min budget |
| `REROLL_SYNC_DOMAIN_RESERVES` | `{"pypi.org": 200, "files.pythonhosted.org": 1800}` (JSON) | per-host reserved share of the budget |
| `REROLL_SYNC_FETCH_WORKERS` | `64` | metadata-download thread pool size |
| `REROLL_SYNC_PROJECT_WORKERS` | `32` | project-page-sync thread pool size |
| `REROLL_SYNC_CONVERT_WORKERS` | `cpu_count - 2` (min 1) | parse/convert process pool size |
| `REROLL_SYNC_HANDOFF_BUDGET_BYTES` | `256 MiB` | in-memory byte budget between fetch and archive/convert |
| `REROLL_SYNC_BATCH_SIZE` / `REROLL_SYNC_BATCH_INTERVAL` | `1000` / `0.1s` | writer-thread batching |
| `REROLL_SYNC_CHECKPOINT_INTERVAL` | `60s` | WAL checkpoint cadence |
| `REROLL_SYNC_VACUUM_INTERVAL` | `3600s` | incremental vacuum cadence |
| `REROLL_SYNC_INDEX_POLL_INTERVAL` | `300s` | how often the simple index is re-polled |
| `REROLL_SYNC_MAX_ATTEMPTS` | `8` | retries before a wheel is quarantined |
| `REROLL_SYNC_BACKOFF_BASE` / `REROLL_SYNC_BACKOFF_CAP` | `30s` / `21600s` | retry backoff schedule |
| `REROLL_SYNC_SEGMENT_SEAL_BYTES` / `REROLL_SYNC_SEGMENT_SEAL_SECONDS` | `64 MiB` / `21600s` | when an open archive segment gets sealed |
| `REROLL_SYNC_DISK_FREE_FLOOR_BYTES` | `20 GiB` | pauses fetch/archive when free disk drops below this |
| `REROLL_SYNC_METRICS_PORT` | unset (disabled) | localhost port to serve Prometheus-style `/metrics` on |
| `REROLL_DATA_BRIDGE_DB_PATH` | unset (disabled) | Location to a reroll-data database, as an alternative source for METADATA files
