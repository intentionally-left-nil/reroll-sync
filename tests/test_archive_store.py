import pytest

from reroll_sync.archive.store import ArchiveStore
from reroll_sync.db import connect_writer, init_db


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "reroll_sync.db"
    init_db(db_path)
    connection = connect_writer(db_path)
    try:
        yield connection
    finally:
        connection.close()


def _mutable_clock(start=0.0):
    state = {"t": start}
    return lambda: state["t"], lambda seconds: state.__setitem__("t", state["t"] + seconds)


def _abandon(writer):
    """Release a writer's file handle without sealing it (avoids a leaked-fd warning)."""
    writer._file.close()


# --- location_for / get -------------------------------------------------------


def test_location_for_unknown_sha256_returns_none(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    assert store.location_for("0" * 64) is None


def test_get_on_unknown_sha256_raises_key_error(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    with pytest.raises(KeyError):
        store.get("0" * 64)


def test_add_then_get_round_trips(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    location = store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())

    assert store.get(location.sha256) == b"metadata bytes"


def test_location_for_matches_what_add_returned(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    location = store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())

    assert store.location_for(location.sha256) == location


# --- read-only introspection: open_writer_if_any / disk_free_bytes -------


def test_open_writer_if_any_is_none_before_any_write(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    assert store.open_writer_if_any() is None


def test_open_writer_if_any_does_not_allocate_a_segment(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    store.open_writer_if_any()
    assert store.sealed_segment_ids() == []
    assert not (tmp_path / "segments").exists() or list((tmp_path / "segments").iterdir()) == []


def test_open_writer_if_any_returns_the_current_writer_once_one_exists(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    writer = store.current_writer()
    assert store.open_writer_if_any() is writer
    _abandon(writer)


def test_open_writer_if_any_is_none_again_after_sealing(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    store.add(b"metadata bytes")
    store.seal_writer(store.current_writer())
    assert store.open_writer_if_any() is None


def test_disk_free_bytes_matches_shutil_disk_usage(tmp_path, conn):
    import shutil

    store = ArchiveStore(tmp_path / "segments", conn)
    expected = shutil.disk_usage(tmp_path / "segments").free
    assert store.disk_free_bytes() == expected


def test_disk_free_bytes_returns_none_for_a_since_deleted_directory(tmp_path, conn):
    import shutil

    store = ArchiveStore(tmp_path / "segments", conn)
    shutil.rmtree(tmp_path / "segments")
    assert store.disk_free_bytes() is None


# --- writer allocation ---------------------------------------------------


def test_current_writer_allocates_one_lazily(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    writer = store.current_writer()
    assert writer is store.current_writer()  # same writer, not reallocated
    _abandon(writer)


def test_open_writer_always_allocates_a_new_segment(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    first = store.open_writer()
    second = store.open_writer()
    assert first.segment_id != second.segment_id
    _abandon(first)
    _abandon(second)


def test_open_writer_inserts_an_unsealed_segments_row(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    writer = store.open_writer()

    row = conn.execute(
        "SELECT sealed_at FROM segments WHERE id = ?", (writer.segment_id,)
    ).fetchone()
    assert row == (None,)
    _abandon(writer)


def test_sealing_updates_the_segments_row(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    store.add(b"one record")
    writer = store.current_writer()

    store.seal_writer(writer)

    row = conn.execute(
        "SELECT sealed_at, bytes, records, footer_sha FROM segments WHERE id = ?",
        (writer.segment_id,),
    ).fetchone()
    sealed_at, bytes_, records, footer_sha = row
    assert sealed_at is not None
    assert bytes_ > 0
    assert records == 1
    assert footer_sha is not None


def test_sealing_clears_the_current_writer_so_the_next_add_opens_a_new_one(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    store.add(b"one record")
    first_writer = store.current_writer()
    store.seal_writer(first_writer)

    store.add(b"another record")
    second_writer = store.current_writer()

    assert second_writer.segment_id != first_writer.segment_id
    _abandon(second_writer)


def test_sealing_a_writer_that_is_not_the_current_one_does_not_clear_current(tmp_path, conn):
    store = ArchiveStore(tmp_path / "segments", conn)
    first = store.open_writer()
    first.add(b"first segment data")
    second = store.open_writer()  # now the tracked current writer

    store.seal_writer(first)

    assert store.current_writer() is second
    _abandon(second)


# --- crash recovery -----------------------------------------------------------


def test_startup_with_open_file_and_unsealed_row_truncates_and_allocates_fresh_id(tmp_path, conn):
    segments_dir = tmp_path / "segments"
    store1 = ArchiveStore(segments_dir, conn)
    writer = store1.open_writer()
    writer.add(b"never sealed")
    stale_segment_id = writer.segment_id
    writer._file.close()  # simulate a crash: release the fd without sealing

    store2 = ArchiveStore(segments_dir, conn)  # simulates a restart

    stale_path = segments_dir / f"{stale_segment_id:06d}.open"
    assert stale_path.exists()
    assert stale_path.stat().st_size == 0

    new_writer = store2.open_writer()
    assert new_writer.segment_id != stale_segment_id
    _abandon(new_writer)


def test_startup_with_open_file_and_missing_row_also_recovers_cleanly(tmp_path, conn):
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    stale_path = segments_dir / "000005.open"
    stale_path.write_bytes(b"leftover garbage bytes, no segments row at all")

    store = ArchiveStore(segments_dir, conn)  # must not raise

    assert stale_path.stat().st_size == 0
    new_writer = store.open_writer()
    assert new_writer.segment_id > 5
    _abandon(new_writer)


def test_startup_does_not_truncate_an_open_file_whose_row_is_actually_sealed(tmp_path, conn):
    # Contrived (the writer never leaves a sealed row with a `.open` file
    # on disk), but recovery must key off the `segments` row, not just the
    # filename suffix -- this pins that down.
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    weird_path = segments_dir / "000009.open"
    weird_path.write_bytes(b"not actually truncated")
    conn.execute(
        "INSERT INTO segments (id, sealed_at, bytes, records, footer_sha) "
        "VALUES (9, '2024-01-01T00:00:00+00:00', 10, 1, 'deadbeef')"
    )
    conn.commit()

    ArchiveStore(segments_dir, conn)

    assert weird_path.read_bytes() == b"not actually truncated"


def test_next_segment_id_skips_ids_already_used_by_sealed_files_not_in_db(tmp_path, conn):
    # A `.zst` file can exist on disk for an id that predates this
    # connection's view of `segments` (e.g. a second store instance
    # sharing the directory); allocation must still not collide with it.
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    (segments_dir / "000003.zst").write_bytes(b"pretend sealed segment")

    store = ArchiveStore(segments_dir, conn)
    writer = store.open_writer()

    assert writer.segment_id > 3
    _abandon(writer)


def test_next_segment_id_ignores_unrelated_files_in_the_directory(tmp_path, conn):
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    (segments_dir / "README.txt").write_text("not a segment file")

    store = ArchiveStore(segments_dir, conn)
    writer = store.open_writer()

    assert writer.segment_id == 0
    _abandon(writer)


# --- recover=False: safe read-only construction against a live daemon ----


def test_recover_false_does_not_truncate_a_stale_open_segment(tmp_path, conn):
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    stale_path = segments_dir / "000005.open"
    stale_path.write_bytes(b"leftover garbage bytes, no segments row at all")

    ArchiveStore(segments_dir, conn, recover=False)

    assert stale_path.read_bytes() == b"leftover garbage bytes, no segments row at all"


def test_recover_false_does_not_truncate_a_live_daemons_actively_open_segment(tmp_path, conn):
    """The exact danger a second, read-only `ArchiveStore` (e.g. from the
    CLI's `verify-archive` command) must not cause: truncating a segment a
    live daemon is still appending to, which looks identical to a crashed
    writer's leftover (`sealed_at IS NULL`) from the outside.
    """
    segments_dir = tmp_path / "segments"
    live_store = ArchiveStore(segments_dir, conn)
    location = live_store.add(b"still being written by the live daemon")

    ArchiveStore(segments_dir, conn, recover=False)  # a second, read-only-intentioned instance

    live_store.seal_writer(live_store.current_writer())
    assert live_store.get(location.sha256) == b"still being written by the live daemon"


def test_recover_true_is_still_the_default(tmp_path, conn):
    segments_dir = tmp_path / "segments"
    segments_dir.mkdir()
    stale_path = segments_dir / "000005.open"
    stale_path.write_bytes(b"leftover garbage bytes, no segments row at all")

    ArchiveStore(segments_dir, conn)

    assert stale_path.stat().st_size == 0


# --- store creates its directory ---------------------------------------------


def test_store_creates_the_segments_directory_if_missing(tmp_path, conn):
    segments_dir = tmp_path / "does" / "not" / "exist" / "yet"
    ArchiveStore(segments_dir, conn)
    assert segments_dir.is_dir()
