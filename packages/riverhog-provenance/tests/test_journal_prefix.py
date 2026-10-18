from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import replace

import pytest
import sha256_continuation as sha_owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    ProvenanceValidationError,
    append_checkpoint,
    assertion,
    encode_entry,
    new_id,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as journal_owner
from riverhog_provenance._journal_prefix import (
    JournalPrefix,
    PrefixState,
    StaleJournalPrefix,
    install,
)
from riverhog_provenance_contracts import ContractCatalog, ProvenanceContractBinding
from sha256_continuation import CheckpointSHA256


@pytest.fixture
def history(journal, who, catalog):
    raw = journal
    for _ in range(2):
        raw = append_checkpoint(
            raw, recorded_by_agent_id=who, purpose="urn:test:prefix", catalog=catalog
        )
    return raw


def connect(path):
    db = sqlite3.connect(path, check_same_thread=False)
    db.execute("PRAGMA synchronous = FULL").close()
    return db


def prefix(db, selected_catalog, **options):
    return JournalPrefix(
        db,
        **{
            "attempt_id": "owned-attempt",
            "source_binding": "owned-immutable-source",
            "catalog": selected_catalog,
            "require_profiles": True,
            **options,
        },
    )


def snapshot(db):
    return {
        table: db.execute("SELECT * FROM " + table).fetchall()
        for table in ("frames", "journal_prefix")
    }


def test_native_validator_publishes_its_exact_prefix_with_each_frame(history, catalog):
    with validate_journal(history, catalog=catalog) as summary:
        db = summary._store.db
        stored = db.execute("SELECT checkpoint FROM journal_prefix").fetchone()[0]
        state = PrefixState.from_json(stored)
        assert state.frame_count == len(summary.frames) == 3
        assert state.byte_count == summary.journal_bytes == len(history)
        assert state.hexdigest == summary.journal_sha256 == hashlib.sha256(history).hexdigest()
        assert state.previous == summary.tail.reference
        assert not db.in_transaction


def test_reopen_resumes_the_exact_prefix_without_reading_old_frame_payloads(
    tmp_path, history, catalog
):
    path = tmp_path / "prefix.sqlite3"
    frames = parse_journal(history)
    with closing(connect(path)) as db:
        install(db)
        prefix(db, catalog).initialize()
    consumed = 0
    for index, frame in enumerate(frames):
        with closing(connect(path)) as db:
            current = prefix(db, catalog)
            before = current.load()
            assert before.frame_count == index
            assert before.byte_count == consumed
            assert before.hexdigest == hashlib.sha256(history[:consumed]).hexdigest()
            statements = []

            def authorize(action, table, column, _database, _trigger):
                if action == sqlite3.SQLITE_READ and table == "frames" and column == "value":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            db.set_trace_callback(statements.append)
            db.set_authorizer(authorize)
            try:
                proposal = current.prepare(frame, catalog)
                assert not db.in_transaction
                after = current.commit(proposal)
            finally:
                db.set_authorizer(None)
                db.set_trace_callback(None)
            consumed += len(frame.encoded)
            assert after.frame_count == index + 1
            assert after.byte_count == consumed
            assert after.hexdigest == hashlib.sha256(history[:consumed]).hexdigest()
            assert not db.in_transaction
            assert not any("COUNT(" in sql or "GROUP BY" in sql for sql in statements)
    with closing(connect(path)) as db:
        assert prefix(db, catalog).finish() == after


@pytest.mark.parametrize("fault", ["attempt", "source", "catalog", "profiles", "anchor", "tail"])
def test_changed_owned_inputs_cannot_reopen_or_publish(tmp_path, journal, catalog, fault):
    frame = parse_journal(journal)[0]
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        prepared = current.prepare(frame, catalog)
        options = {}
        if fault == "attempt":
            options["attempt_id"] = "replacement"
        elif fault == "source":
            options["source_binding"] = "replacement"
        elif fault == "catalog":
            options["catalog"] = ContractCatalog(
                (
                    ProvenanceContractBinding(
                        contract_id="urn:test:other:contract",
                        schemas=[
                            {
                                "$id": "urn:test:other:schema",
                                "$schema": "https://json-schema.org/draft/2020-12/schema",
                                "type": "object",
                            }
                        ],
                    ),
                )
            )
        elif fault == "profiles":
            options["require_profiles"] = False
        elif fault == "anchor":
            with validate_journal(journal, catalog=catalog) as summary:
                options["expected_anchor"] = summary.anchor
        else:
            options["require_exact_tail"] = True
        changed = prefix(db, catalog, **options)
        before = snapshot(db)
        with pytest.raises(StaleJournalPrefix):
            changed.load()
        with pytest.raises(StaleJournalPrefix):
            changed.commit(prepared)
        assert snapshot(db) == before and not db.in_transaction


@pytest.mark.parametrize(
    "fault",
    ["extra", "algorithm", "scalar", "hash-position", "predecessor", "hash-codec", "oversized"],
)
def test_corrupt_checkpoint_fails_closed_without_reset(tmp_path, journal, catalog, fault):
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        state = current.commit(current.prepare(parse_journal(journal)[0], catalog))
        value = json.loads(state.to_json())
        if fault == "extra":
            value["extra"] = True
        elif fault == "algorithm":
            value["algorithm"] = "other"
        elif fault == "scalar":
            value["frame_count"] = "01"
        elif fault == "hash-position":
            value["byte_count"] = str(state.byte_count + 1)
        elif fault == "predecessor":
            value["previous"]["sequence"] = "7"
        elif fault == "hash-codec":
            value["sha256_state"] = CheckpointSHA256(b"other").export_state()
        else:
            value["unknown"] = "x" * 4096
        db.execute(
            "UPDATE journal_prefix SET checkpoint = ?", (canonical_json_bytes(value).decode(),)
        )
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError, match="checkpoint"):
            current.load()
        assert snapshot(db) == before


@pytest.mark.parametrize("fault", ["missing-header", "missing-predecessor", "changed-predecessor"])
def test_lost_manifest_links_are_explicit_failures_not_cache_misses(
    tmp_path, journal, catalog, fault
):
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        current.commit(current.prepare(parse_journal(journal)[0], catalog))
        if fault == "missing-header":
            db.execute("DELETE FROM journal_prefix")
        elif fault == "missing-predecessor":
            db.execute("DELETE FROM frames")
        else:
            db.execute("UPDATE frames SET reference = ?", (b"changed",))
        db.commit()
        before = snapshot(db)
        with pytest.raises(RuntimeError):
            current.load()
        with pytest.raises((RuntimeError, sqlite3.IntegrityError)):
            current.initialize()
        assert snapshot(db) == before


def test_entry_preparation_is_detached_from_write_transactions(tmp_path, journal, catalog):
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        db.execute("CREATE TABLE owner_work(value TEXT)")
        db.execute("INSERT INTO owner_work VALUES ('pending')")
        with pytest.raises(RuntimeError, match="transaction"):
            current.prepare(parse_journal(journal)[0], catalog)
        db.rollback()


def test_stale_proposal_rolls_back_only_its_uncommitted_owner_writes(tmp_path, journal, catalog):
    path = tmp_path / "prefix.sqlite3"
    frame = parse_journal(journal)[0]
    with closing(connect(path)) as first, closing(connect(path)) as second:
        install(first)
        first.execute("CREATE TABLE owner_work(value TEXT)")
        old = prefix(first, catalog)
        old.initialize()
        proposal = old.prepare(frame, catalog)
        winner = prefix(second, catalog)
        accepted = winner.commit(winner.prepare(frame, catalog))
        first.execute("INSERT INTO owner_work VALUES ('discard')")
        with pytest.raises(StaleJournalPrefix, match="superseded"):
            old.commit(proposal)
        assert not first.in_transaction and old.load() == accepted
        assert first.execute("SELECT * FROM owner_work").fetchall() == []
        assert first.execute("SELECT COUNT(*) FROM frames").fetchone()[0] == 1


def test_two_real_connections_commit_only_one_competing_prefix(tmp_path, journal, catalog):
    path = tmp_path / "prefix.sqlite3"
    with closing(connect(path)) as db:
        install(db)
        prefix(db, catalog).initialize()
    barrier = threading.Barrier(2)
    frame = parse_journal(journal)[0]

    def worker():
        with closing(connect(path)) as db:
            current = prefix(db, catalog)
            proposal = current.prepare(frame, catalog)
            barrier.wait(timeout=10)
            try:
                return current.commit(proposal).frame_count
            except StaleJournalPrefix:
                return "stale"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: worker(), range(2)))
    assert sorted(results, key=str) == [1, "stale"]
    with closing(connect(path)) as db:
        assert prefix(db, catalog).finish().frame_count == 1
        assert db.execute("SELECT COUNT(*) FROM frames").fetchone()[0] == 1


def test_late_frame_insert_failure_rolls_back_header_frame_and_owner_writes(
    tmp_path, journal, catalog
):
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        db.execute("CREATE TABLE owner_work(value TEXT)")
        current = prefix(db, catalog)
        current.initialize()
        prepared = current.prepare(parse_journal(journal)[0], catalog)
        before = snapshot(db)
        db.execute("INSERT INTO owner_work VALUES ('discard')")

        def authorize(action, table, _column, _database, _trigger):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_INSERT and table == "frames"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorize)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                current.commit(prepared)
        finally:
            db.set_authorizer(None)
        assert snapshot(db) == before and not db.in_transaction
        assert db.execute("SELECT * FROM owner_work").fetchall() == []


CRASH = r"""
import os
import sqlite3
import sys
from pathlib import Path
from riverhog_provenance import parse_journal
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance_contracts import ContractCatalog

point, path, source = sys.argv[1:]
class Connection(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        cursor = super().execute(sql, parameters)
        if (point == 'after-header' and sql.startswith('UPDATE journal_prefix')) or (
            point == 'after-frame' and sql.startswith('INSERT INTO frames')
        ):
            os._exit(73)
        return cursor
    def commit(self):
        if point == 'before-commit':
            os._exit(73)
        result = super().commit()
        if point == 'after-commit':
            os._exit(73)
        return result
db = sqlite3.connect(path, factory=Connection)
db.execute('PRAGMA synchronous = FULL').close()
catalog = ContractCatalog()
current = JournalPrefix(db, attempt_id='owned-attempt', source_binding='owned-immutable-source',
                        catalog=catalog, require_profiles=True)
state = current.load()
raw = Path(source).read_bytes()[state.byte_count:]
proposal = current.prepare(next(iter(parse_journal(raw))), catalog)
if point == 'after-prepare':
    os._exit(73)
current.commit(proposal)
raise AssertionError('crash point was not reached')
"""


@pytest.mark.parametrize(
    "point", ["after-prepare", "after-header", "after-frame", "before-commit", "after-commit"]
)
def test_process_exit_keeps_frame_hash_predecessor_and_watermark_atomic(
    tmp_path, history, catalog, point
):
    path = tmp_path / "prefix.sqlite3"
    source = tmp_path / "source.json-seq"
    source.write_bytes(history)
    frames = parse_journal(history)
    with closing(connect(path)) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        first = current.commit(current.prepare(frames[0], catalog))
    result = subprocess.run(
        [sys.executable, "-c", CRASH, point, str(path), str(source)], capture_output=True
    )
    assert result.returncode == 73, result.stderr.decode()
    with closing(connect(path)) as db:
        current = prefix(db, catalog)
        saved = current.load()
        assert saved.frame_count == (2 if point == "after-commit" else 1)
        if point != "after-commit":
            assert saved == first
        assert db.execute("SELECT COUNT(*) FROM frames").fetchone()[0] == saved.frame_count
        assert saved.hexdigest == hashlib.sha256(history[: saved.byte_count]).hexdigest()
        for frame in frames[saved.frame_count :]:
            current.commit(current.prepare(frame, catalog))
        assert current.finish().hexdigest == hashlib.sha256(history).hexdigest()


def test_native_graph_error_precedes_anchor_mismatch_and_later_parse_error(
    journal, catalog, monkeypatch
):
    document = parse_journal(journal)[0].document
    document["recorded_by_agent_id"] = new_id()
    raw = encode_entry(document, catalog=catalog)
    expected = {
        "journal_id": document["journal_id"],
        "through": parse_journal(raw)[0].reference,
        "prefix_sha256": "0" * 64,
        "prefix_bytes": str(len(raw)),
    }
    calls = []
    original = JournalPrefix.commit

    def commit(self, proposal):
        calls.append(proposal)
        return original(self, proposal)

    monkeypatch.setattr(JournalPrefix, "commit", commit)
    with pytest.raises(ProvenanceValidationError, match="entry recorder"):
        validate_journal(raw + b"invalid later frame", catalog=catalog, expected_anchor=expected)
    assert calls == []


def test_anchor_match_is_retained_across_restart_and_exact_tail_stays_strict(
    tmp_path, history, catalog
):
    frames = parse_journal(history)
    with validate_journal(frames[0].encoded, catalog=catalog) as summary:
        anchor = summary.anchor
    path = tmp_path / "prefix.sqlite3"
    with closing(connect(path)) as db:
        install(db)
        current = prefix(db, catalog, expected_anchor=anchor, require_exact_tail=True)
        current.initialize()
        current.commit(current.prepare(frames[0], catalog))
        assert current.finish().matched_anchor
    with closing(connect(path)) as db:
        current = prefix(db, catalog, expected_anchor=anchor, require_exact_tail=True)
        assert current.load().matched_anchor
        current.commit(current.prepare(frames[1], catalog))
        with pytest.raises(ProvenanceValidationError, match="exact tail"):
            current.finish()


def test_owning_scalar_width_and_codec_position_are_preserved():
    count = 2**40
    digest = CheckpointSHA256()
    digest._total_bytes = count * 64
    previous = {"entry_id": new_id(), "sequence": str(count - 1), "json_sha256": "f" * 64}
    state = PrefixState(
        count,
        count * 64,
        new_id(),
        canonical_json_bytes(previous).decode(),
        digest.export_state(),
        False,
    )
    assert PrefixState.from_json(state.to_json()) == state
    with pytest.raises(RuntimeError, match="checkpoint"):
        PrefixState.from_json(replace(state, byte_count=state.byte_count + 1).to_json())


class PrefixCursor(sqlite3.Cursor):
    closed = False

    def fetchone(self):
        row = super().fetchone()
        if row is not None:
            self.connection.metrics["returned_rows"] += 1
            self.connection.metrics["metadata_octets"] += sum(
                len(value.encode() if isinstance(value, str) else value) for value in row
            )
        return row

    def close(self):
        self.closed = True
        return super().close()


class PrefixConnection(sqlite3.Connection):
    recording = False

    def execute(self, sql, parameters=()):
        if not self.recording:
            return super().execute(sql, parameters)
        cursor = self.cursor(factory=PrefixCursor)
        self.statements.append((sql, parameters, cursor))
        result = cursor.execute(sql, parameters)
        if sql.startswith("UPDATE journal_prefix"):
            self.metrics["published_metadata_octets"] += len(parameters[0].encode())
        elif sql.startswith("INSERT INTO frames"):
            self.metrics["published_payload_octets"] += len(parameters[1])
            self.metrics["published_metadata_octets"] += len(parameters[0].encode()) + len(
                parameters[2]
            )
        return result

    @contextmanager
    def observe(self):
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "metadata_octets",
                "changed_rows",
                "published_metadata_octets",
                "published_payload_octets",
                "source_hashed_octets",
                "native_hashed_octets",
                "sha256_compression_calls",
                "sqlite_vm_instructions",
            )
        }
        self.statements = []
        changes = self.total_changes

        def trace(_sql):
            self.metrics["sql_statements"] += 1

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and (
                table not in {"frames", "journal_prefix"} or table == "frames" and column == "value"
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.set_trace_callback(trace)
        self.set_progress_handler(tick, 1)
        self.set_authorizer(authorize)
        self.recording = True
        try:
            yield self.metrics
        finally:
            self.recording = False
            self.set_trace_callback(None)
            self.set_progress_handler(None, 0)
            self.set_authorizer(None)
            self.metrics["changed_rows"] = self.total_changes - changes
        assert not self.in_transaction
        assert all(cursor.closed for _sql, _parameters, cursor in self.statements)
        for sql, parameters, _cursor in self.statements:
            if not sql.startswith(("SELECT", "UPDATE", "DELETE")):
                continue
            with closing(self.execute("EXPLAIN QUERY PLAN " + sql, parameters)) as cursor:
                plan = " ".join(row[3] for row in cursor.fetchall())
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


def checkpoint_frame(state, who, catalog):
    document = journal_owner._entry(
        journal_id=state.journal_id,
        recorder_id=who,
        kind="checkpoint",
        body={
            "covered_through": state.previous,
            "prefix_sha256": state.hexdigest,
            "prefix_bytes": str(state.byte_count),
            "purpose": "urn:test:prefix",
        },
        sequence=state.frame_count,
        previous=state.previous,
    )
    return parse_journal(encode_entry(document, catalog=catalog))[0]


@pytest.mark.growth_witness(
    "riverhog-journal-prefix-preparation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._journal_prefix.JournalPrefix.prepare",
)
def test_actual_cold_prefix_preparation_work_and_shared_hash_composition(
    tmp_path, journal, who, catalog, monkeypatch, record_property
):
    binding = growth_binding(JournalPrefix.prepare)
    update_binding = growth_binding(CheckpointSHA256.update_bytes)
    compression_binding = growth_binding(sha_owner.sha256_compress)
    projection = project_bindings((binding,))
    sample_hash = hashlib.sha256

    def sample(owner, dimensions, metrics, delegates):
        return {
            "declaration": owner.declaration.id,
            "declaration_sha256": sample_hash(
                canonical_json_bytes(projection["declarations"][owner.declaration.id])
            ).hexdigest(),
            "dimensions": dimensions,
            "metrics": {bound.metric: metrics[bound.metric] for bound in owner.declaration.bounds},
            "delegates": delegates,
        }

    class Buffer(bytearray):
        def __init__(self, value):
            super().__init__(value)
            self.peak = len(self)

        def extend(self, value):
            super().extend(value)
            self.peak = max(self.peak, len(self))

    samples = []
    for count, payload, kind in (
        (1, 1, "assertion"),
        (17, 1, "assertion"),
        (129, 1, "assertion"),
        (1025, 1, "assertion"),
        (1, 65537, "assertion"),
        (1025, 65537, "assertion"),
        (17, 0, "checkpoint"),
        (129, 0, "checkpoint"),
    ):
        path = tmp_path / f"{count}-{payload}-{kind}.sqlite3"
        with closing(connect(":memory:")) as seed:
            install(seed)
            current = prefix(seed, catalog)
            current.initialize()
            first = parse_journal(journal)[0]
            state = current.commit(current.prepare(first, catalog))
            source = [first.encoded]
            for _ in range(1, count):
                frame = checkpoint_frame(state, who, catalog)
                state = current.commit(current.prepare(frame, catalog))
                source.append(frame.encoded)
            if kind == "checkpoint":
                candidate = checkpoint_frame(state, who, catalog)
            else:
                target = first.document["body"]["assertions"]["artifacts"][0]["id"]
                row = assertion(
                    "extension",
                    who,
                    subject=reference(target, "artifact"),
                    property="urn:test:prefix-property",
                    value={"type": "text", "value": "x" * payload},
                )
                document = journal_owner._entry(
                    journal_id=state.journal_id,
                    recorder_id=who,
                    kind="assertion",
                    body={"assertions": {"extensions": [row]}},
                    sequence=state.frame_count,
                    previous=state.previous,
                )
                candidate = parse_journal(encode_entry(document, catalog=catalog))[0]
            with closing(connect(path)) as stored:
                seed.backup(stored)
        with closing(sqlite3.connect(path, factory=PrefixConnection)) as db:
            db.execute("PRAGMA synchronous = FULL").close()
            current = prefix(db, catalog)
            compressions, updates = [], []
            rotates = [0]
            real_compress, real_rotate = sha_owner.sha256_compress, sha_owner._rotr
            real_update, real_native = CheckpointSHA256.update_bytes, hashlib.sha256

            def rotate(value, shift, *, rotates=rotates, real_rotate=real_rotate):
                rotates[0] += 1
                return real_rotate(value, shift)

            def compress(
                words,
                block,
                *,
                rotates=rotates,
                real_compress=real_compress,
                compressions=compressions,
                db=db,
            ):
                before = rotates[0]
                result = real_compress(words, block)
                compressions.append(
                    sample(
                        compression_binding,
                        {"block_octets": len(block)},
                        {
                            "block_input_octets": len(block),
                            "rotate_calls": rotates[0] - before,
                            "returned_words": len(result),
                        },
                        {},
                    )
                )
                db.metrics["sha256_compression_calls"] += 1
                return result

            def update(
                digest,
                data,
                *,
                compressions=compressions,
                real_update=real_update,
                updates=updates,
                db=db,
            ):
                first_call = len(compressions)
                initial = len(digest._buffer)
                digest._buffer = Buffer(digest._buffer)
                result = real_update(digest, data)
                if data:
                    calls = compressions[first_call:]
                    db.metrics["source_hashed_octets"] += len(data)
                    updates.append(
                        sample(
                            update_binding,
                            {
                                "input_octets": len(data),
                                "initial_tail_octets": initial,
                                "blocks": (initial + len(data)) // 64,
                            },
                            {
                                "compression_calls": len(calls),
                                "block_input_octets": sum(
                                    c["metrics"]["block_input_octets"] for c in calls
                                ),
                                "rotate_calls": sum(c["metrics"]["rotate_calls"] for c in calls),
                                "pending_buffer_octets": digest._buffer.peak,
                                "retained_tail_octets": len(digest._buffer),
                            },
                            {"compression": calls},
                        )
                    )
                return result

            def native(data=b"", *, real_native=real_native, db=db, **options):
                db.metrics["native_hashed_octets"] += len(data)
                return real_native(data, **options)

            dimensions = {
                "retained_frames": count,
                "frame_octets": len(candidate.encoded),
                "initial_tail_octets": state.byte_count % 64,
                "source_blocks": (state.byte_count % 64 + len(candidate.encoded)) // 64,
            }
            with db.observe() as metrics:
                with monkeypatch.context() as patch:
                    patch.setattr(sha_owner, "_rotr", rotate)
                    patch.setattr(sha_owner, "sha256_compress", compress)
                    patch.setattr(CheckpointSHA256, "update_bytes", update)
                    patch.setattr(hashlib, "sha256", native)
                    prepared = current.prepare(candidate, catalog)
            observed = sample(binding, dimensions, metrics, {"source_update": updates})
            validate_execution_samples(projection, binding.declaration.id, [observed])
            samples.append(observed)
            assert len(updates) == 1
            assert current.load() == state
            assert (
                CheckpointSHA256.from_state(prepared.sha256_state).hexdigest()
                == hashlib.sha256(b"".join(source) + candidate.encoded).hexdigest()
            )
    record_property("growth_samples", samples)


def test_replaced_attempt_fences_an_already_prepared_worker(tmp_path, journal, catalog):
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        old = prefix(db, catalog)
        old.initialize()
        frame = parse_journal(journal)[0]
        proposal = old.prepare(frame, catalog)
        db.execute("UPDATE journal_prefix SET attempt_id = 'replacement'")
        db.commit()
        replacement = prefix(db, catalog, attempt_id="replacement")
        before = snapshot(db)
        with pytest.raises(StaleJournalPrefix, match="superseded"):
            old.commit(proposal)
        assert snapshot(db) == before and not db.in_transaction
        assert replacement.commit(replacement.prepare(frame, catalog)).frame_count == 1
        with pytest.raises(StaleJournalPrefix):
            old.load()


def test_anchor_mismatch_rolls_back_the_pending_owner_transaction(tmp_path, journal, catalog):
    with validate_journal(journal, catalog=catalog) as summary:
        anchor = {**summary.anchor, "prefix_sha256": "0" * 64}
    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        db.execute("CREATE TABLE owner_work(value TEXT)")
        current = prefix(db, catalog, expected_anchor=anchor)
        current.initialize()
        proposal = current.prepare(parse_journal(journal)[0], catalog)
        before = snapshot(db)
        db.execute("INSERT INTO owner_work VALUES ('discard')")
        with pytest.raises(ProvenanceValidationError, match="externally supplied prefix anchor"):
            current.commit(proposal)
        assert snapshot(db) == before and not db.in_transaction
        assert db.execute("SELECT * FROM owner_work").fetchall() == []


def test_existing_entry_limit_is_checked_before_loading_or_decoding(tmp_path, catalog):
    from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    from riverhog_provenance.journal import JournalFrame

    with closing(connect(tmp_path / "prefix.sqlite3")) as db:
        install(db)
        current = prefix(db, catalog)
        current.initialize()
        frame = JournalFrame(b"x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1))
        statements = []
        db.set_trace_callback(statements.append)
        try:
            with pytest.raises(ProvenanceValidationError, match="entry exceeds"):
                current.prepare(frame, catalog)
        finally:
            db.set_trace_callback(None)
        assert not statements and current.load() == PrefixState.empty()


@pytest.mark.growth_witness(
    "riverhog-journal-prefix-publication-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._journal_prefix.JournalPrefix.commit",
)
def test_actual_cold_prefix_publication_work(
    tmp_path, journal, who, catalog, monkeypatch, record_property
):
    binding = growth_binding(JournalPrefix.commit)
    projection = project_bindings((binding,))
    declaration_sha256 = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for count, payload, anchored in (
        (1, 1, False),
        (17, 1, False),
        (129, 1, False),
        (1025, 1, False),
        (1, 65537, False),
        (1025, 65537, False),
        (17, 0, True),
        (129, 0, True),
    ):
        path = tmp_path / f"publication-{count}-{payload}-{anchored}.sqlite3"
        with closing(connect(":memory:")) as seed:
            install(seed)
            current = prefix(seed, catalog)
            current.initialize()
            first = parse_journal(journal)[0]
            frames = [first]
            state = current.commit(current.prepare(first, catalog))
            for _ in range(1, count):
                frame = checkpoint_frame(state, who, catalog)
                state = current.commit(current.prepare(frame, catalog))
                frames.append(frame)
            if anchored:
                candidate = checkpoint_frame(state, who, catalog)
            else:
                target = first.document["body"]["assertions"]["artifacts"][0]["id"]
                row = assertion(
                    "extension",
                    who,
                    subject=reference(target, "artifact"),
                    property="urn:test:prefix-property",
                    value={"type": "text", "value": "x" * payload},
                )
                candidate = parse_journal(
                    encode_entry(
                        journal_owner._entry(
                            journal_id=state.journal_id,
                            recorder_id=who,
                            kind="assertion",
                            body={"assertions": {"extensions": [row]}},
                            sequence=state.frame_count,
                            previous=state.previous,
                        ),
                        catalog=catalog,
                    )
                )[0]
            raw = b"".join(frame.encoded for frame in frames) + candidate.encoded
            expected_digest = hashlib.sha256(raw).hexdigest()
            with validate_journal(raw, catalog=catalog) as verified:
                assert verified.journal_sha256 == expected_digest
            anchor = (
                None
                if not anchored
                else {
                    "journal_id": state.journal_id,
                    "through": candidate.reference,
                    "prefix_sha256": expected_digest,
                    "prefix_bytes": str(len(raw)),
                }
            )
            # Bind a target anchor before accepting any frame in its workspace.
            # Both workspaces use the maintained prefix transitions for fixture setup.
            if anchored:
                with closing(connect(":memory:")) as bound:
                    install(bound)
                    current = prefix(bound, catalog, expected_anchor=anchor)
                    current.initialize()
                    for frame in frames:
                        current.commit(current.prepare(frame, catalog))
                    proposal = current.prepare(candidate, catalog)
                    with closing(connect(path)) as stored:
                        bound.backup(stored)
            else:
                proposal = current.prepare(candidate, catalog)
                with closing(connect(path)) as stored:
                    seed.backup(stored)
        with closing(sqlite3.connect(path, factory=PrefixConnection)) as db:
            db.execute("PRAGMA synchronous = FULL").close()
            current = prefix(db, catalog, expected_anchor=anchor)
            real_update, real_compress = CheckpointSHA256.update_bytes, sha_owner.sha256_compress

            def update(digest, data, *, real_update=real_update, db=db):
                db.metrics["source_hashed_octets"] += len(data)
                return real_update(digest, data)

            def compress(words, block, *, real_compress=real_compress, db=db):
                db.metrics["sha256_compression_calls"] += 1
                return real_compress(words, block)

            with db.observe() as metrics:
                with monkeypatch.context() as patch:
                    patch.setattr(CheckpointSHA256, "update_bytes", update)
                    patch.setattr(sha_owner, "sha256_compress", compress)
                    accepted = current.commit(proposal)
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": declaration_sha256,
                "dimensions": {"retained_frames": count, "frame_octets": len(candidate.encoded)},
                "metrics": {
                    bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
            assert accepted.hexdigest == expected_digest and accepted.frame_count == count + 1
            assert accepted.byte_count == len(raw) and accepted.matched_anchor == anchored
            assert current.load() == accepted
            assert (
                db.execute("SELECT value FROM frames WHERE sequence = ?", (str(count),)).fetchone()[
                    0
                ]
                == candidate.json_bytes
            )
            assert metrics["source_hashed_octets"] == 0
    record_property("growth_samples", samples)
