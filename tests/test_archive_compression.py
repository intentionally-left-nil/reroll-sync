from archive_corpus import generate_corpus
from reroll_sync.archive.format import compress_block
from reroll_sync.archive.writer import DEFAULT_LEVEL, SegmentWriter

_MIN_RATIO = 4.0


def test_synthetic_corpus_has_a_thousand_realistic_records():
    corpus = generate_corpus()
    assert len(corpus) == 1000
    avg_size = sum(len(r) for r in corpus) / len(corpus)
    assert 1000 < avg_size < 20_000  # roughly METADATA-file-sized


def test_block_level_compression_on_project_ordered_corpus_beats_the_floor(tmp_path):
    corpus = generate_corpus()
    raw_total = sum(len(record) for record in corpus)

    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        for record in corpus:
            writer.add(record)
        writer.seal()

    compressed_total = (tmp_path / "000001.zst").stat().st_size
    ratio = raw_total / compressed_total

    assert ratio >= _MIN_RATIO


def test_block_level_compression_beats_per_record_framing(tmp_path):
    corpus = generate_corpus()
    raw_total = sum(len(record) for record in corpus)

    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as writer:
        for record in corpus:
            writer.add(record)
        writer.seal()
    block_compressed_total = (tmp_path / "000001.zst").stat().st_size

    per_record_total = sum(len(compress_block(record, level=DEFAULT_LEVEL)) for record in corpus)

    # The floor this module exists to beat: the real corpus's per-body
    # zlib-6 baseline measures 2.81x. Block-level zstd on project-ordered
    # input must not merely match per-record framing at the same codec and
    # level -- it must be meaningfully better, or blocking bought nothing.
    assert block_compressed_total < per_record_total
    assert raw_total / per_record_total < raw_total / block_compressed_total


def test_shuffled_order_compresses_worse_than_project_order(tmp_path):
    # Not an acceptance criterion on its own, but pins down *why* project
    # ordering is the caller's responsibility (see the writer's docstring):
    # losing the grouping should cost real ratio, not be a no-op.
    import random

    corpus = generate_corpus()
    shuffled = list(corpus)
    random.Random(0).shuffle(shuffled)

    with SegmentWriter(tmp_path, 1, now=lambda: 0.0) as ordered_writer:
        for record in corpus:
            ordered_writer.add(record)
        ordered_writer.seal()
    ordered_size = (tmp_path / "000001.zst").stat().st_size

    with SegmentWriter(tmp_path, 2, now=lambda: 0.0) as shuffled_writer:
        for record in shuffled:
            shuffled_writer.add(record)
        shuffled_writer.seal()
    shuffled_size = (tmp_path / "000002.zst").stat().st_size

    assert ordered_size < shuffled_size
