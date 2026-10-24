from __future__ import annotations

import builtins
import json
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
import riverhog_provenance.corpus as native
import riverhog_provenance.corpus_workspace as retained
import riverhog_provenance.journal as journal
import riverhog_provenance.journal_store as journal_store
import sha256_continuation as sha_owner
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    CanonicalCorpusValidator,
    IncompleteSourceError,
    RetainedCorpusValidator,
    validate_journal,
)
from riverhog_provenance.journal_store import _Frames
from test_journal_result import history
from test_retained_corpus_workspace import OwnedCorpusSource, open_workspace
from test_source_anchor_growth import record

ROOT = RetainedCorpusValidator.advance_source
NATIVE = CanonicalCorpusValidator.advance_source
TRANSACTION = RetainedCorpusValidator._operation


def octets(value):
    if isinstance(value, (tuple, list)):
        return sum(map(octets, value))
    if isinstance(value, bytes):
        return len(value)
    return 0 if value is None else len(str(value).encode())


class Cursor:
    def __init__(self, actual, observer, key):
        self.actual, self.observer, self.key, self.closed = actual, observer, key, False
        observer.live += 1
        observer.max_live = max(observer.max_live, observer.live)

    @property
    def rowcount(self):
        return self.actual.rowcount

    def fetchone(self):
        row = self.actual.fetchone()
        if row is not None:
            if self.key == "corpus:identity":
                assert len(row) == 1 and type(row[0]) is int and row[0] in (0, 1)
            self.observer.metrics[self.key]["returned_rows"] += 1
            self.observer.metrics[self.key]["returned_octets"] += octets(row)
        return row

    def close(self):
        if not self.closed:
            self.actual.close()
            self.observer.metrics[self.key]["closed_cursors"] += 1
            self.closed = True
            self.observer.live -= 1

    def __del__(self):
        if not self.closed:
            self.observer.implicit_releases += 1
            self.close()


class Connection:
    def __init__(self, actual, observer, kind):
        self.actual, self.observer, self.kind = actual, observer, kind

    def execute(self, query, parameters=()):
        key = self.kind + ":" + self.observer.stage
        metrics = self.observer.metrics[key]
        metrics["sql_statements"] += 1
        metrics["parameter_octets"] += octets(parameters)
        if query.startswith("SELECT"):
            metrics["comparison_input_octets"] += sum(
                len(v) for v in parameters if isinstance(v, bytes)
            )
        self.observer.queries.append((self.kind, query, parameters))
        actual = self.actual.execute(query, parameters)
        if query.startswith(("INSERT", "UPDATE", "DELETE")) and actual.rowcount > 0:
            metrics["written_rows"] += actual.rowcount
            metrics["written_parameter_octets"] += octets(parameters)
        return Cursor(actual, self.observer, key)

    def commit(self):
        self.observer.metrics[self.kind + ":" + self.observer.stage]["sql_statements"] += 1
        self.actual.commit()

    def rollback(self):
        self.observer.metrics[self.kind + ":" + self.observer.stage]["sql_statements"] += 1
        self.actual.rollback()


class Observer:
    def __init__(self):
        self.metrics = defaultdict(lambda: defaultdict(int))
        self.stage = "owner"
        self.live = self.max_live = self.implicit_releases = 0
        self.frames = []
        self.identity_samples = []
        self.header_samples = []
        self.parent_headers = []
        self.queries = []
        self.padding = []

    @contextmanager
    def stage_as(self, value):
        old, self.stage = self.stage, value
        try:
            yield
        finally:
            self.stage = old

    @contextmanager
    def observe(self, workspace, source, summary):
        db, raw = workspace._db, summary._store._storage.connection
        advance = CanonicalCorpusValidator.advance_source
        identity = CanonicalCorpusValidator._identity
        register = workspace._register
        admit, current = source.admit_source, source.verify_current
        item = _Frames.__getitem__
        decoder = journal.decode_document
        canonical = native.canonical_document
        parent_encoded = retained.canonical_json_bytes
        header_encoded = journal.canonical_document
        header_load = journal.json.loads
        entry_hash = journal.JournalFrame.sha256.fget
        checkpoint, restore = retained._checkpoint, retained._restore

        def checkpointed(corpus):
            value = checkpoint(corpus)
            self.metrics["control"]["checkpoint_encoded_octets"] += len(value)
            return value

        def restored_checkpoint(corpus, value):
            self.metrics["control"]["checkpoint_decoded_octets"] += len(value)
            return restore(corpus, value)

        anchor_root = journal.JournalSummary.anchor_at
        compress, rotate = sha_owner.sha256_compress, sha_owner._rotr
        # Fixture dimensions precede observation; the operation never computes this inventory.
        headers = {}
        for sequence, reference, state in raw.execute(
            "SELECT sequence,reference,prefix_state FROM frames"
        ):
            restored = sha_owner.CheckpointSHA256.from_state(state)
            headers[int(sequence)] = {
                "retained_frames": len(summary.frames),
                "sequence_octets": len(sequence),
                "header_octets": octets(reference) + octets(state),
                "padding_blocks": 1 if restored.total_bytes % 64 < 56 else 2,
            }
        identity_extent = (
            db.execute("SELECT count(*) FROM identities").fetchone()[0]
            + 1
            + len(summary.frames)
            + 2
            * sum(
                sum(len(v) for v in frame.document["body"].get("assertions", {}).values())
                for frame in summary.frames
            )
        )

        def progress(kind):
            def stepped():
                self.metrics[kind + ":" + self.stage]["sqlite_vm_instructions"] += 1
                return 0

            return stepped

        def algorithm(corpus, *args, **kwargs):
            self.metrics["control"]["native_calls"] += 1
            with self.stage_as("native"):
                return advance(corpus, *args, **kwargs)

        def identified(corpus, key, domain, value):
            self.metrics["control"]["identity_calls"] += 1
            before = self.metrics["corpus:identity"].copy()
            try:
                with self.stage_as("identity"):
                    return identity(corpus, key, domain, value)
            finally:
                metrics = defaultdict(
                    int, {k: v - before[k] for k, v in self.metrics["corpus:identity"].items()}
                )
                self.identity_samples.append(
                    record(
                        growth_binding(identity),
                        {
                            "retained_identities": identity_extent,
                            "identity_octets": octets(key),
                            "domain_octets": octets(domain),
                            "preimage_octets": len(value),
                        },
                        metrics,
                    )
                )

        def anchored(current, sequence):
            parent = self.stage == "owner"
            before = self.metrics["source:header"].copy()
            codec_before = self.metrics["codec:header"].copy()
            padding_before = len(self.padding)
            try:
                with self.stage_as("header"):
                    return anchor_root(current, sequence)
            finally:
                physical = defaultdict(
                    int, {k: v - before[k] for k, v in self.metrics["source:header"].items()}
                )
                codecs = defaultdict(
                    int, {k: v - codec_before[k] for k, v in self.metrics["codec:header"].items()}
                )
                metrics = defaultdict(int, physical)
                metrics.update(
                    metadata_octets=physical["returned_octets"],
                    bound_parameter_octets=physical["parameter_octets"],
                    decoded_octets=codecs["header_decoded_octets"],
                    encoded_octets=codecs["canonical_encoded_octets"],
                    json_decodes=codecs["json_decodes"],
                    json_encodes=codecs["json_encodes"],
                    compression_calls=len(self.padding) - padding_before,
                )
                children = [
                    record(growth_binding(compress), {"block_octets": 64}, v)
                    for v in self.padding[padding_before:]
                ]
                for metric in ("block_input_octets", "rotate_calls", "returned_words"):
                    metrics["padding_" + metric] = sum(v["metrics"][metric] for v in children)
                (self.parent_headers if parent else self.header_samples).append(
                    record(
                        growth_binding(anchor_root),
                        headers[sequence],
                        metrics,
                        {"padding": children},
                    )
                )

        def compressed(state, block):
            assert self.live == 0
            self.padding.append(
                {"block_input_octets": len(block), "rotate_calls": 0, "returned_words": 0}
            )
            value = compress(state, block)
            self.padding[-1]["returned_words"] = len(value)
            return value

        def rotated(value, distance):
            self.padding[-1]["rotate_calls"] += 1
            return rotate(value, distance)

        def registered(*args, **kwargs):
            self.metrics["control"]["registration_calls"] += 1
            with self.stage_as("registration"):
                return register(*args, **kwargs)

        def admitted(anchor):
            self.metrics["control"]["manifest_checks"] += 1
            return admit(anchor)

        def checked():
            self.metrics["control"]["source_checks"] += 1
            return current()

        def frame(frames, index):
            assert self.stage not in ("identity", "header")
            value = item(frames, index)
            assert isinstance(index, int)
            self.frames.append((index, len(value.encoded)))
            return value

        def decoded(value):
            self.metrics["codec:" + self.stage]["document_decoded_octets"] += len(value)
            return decoder(value)

        def loads(value):
            self.metrics["codec:" + self.stage]["header_decoded_octets"] += octets(value)
            self.metrics["codec:" + self.stage]["json_decodes"] += 1
            return header_load(value)

        def encoded_result(result):
            self.metrics["codec:" + self.stage]["canonical_encoded_octets"] += octets(result)
            self.metrics["codec:" + self.stage]["json_encodes"] += 1
            return result

        def encoded(call, value):
            return encoded_result(call(value))

        def dumped(*args, **kwargs):
            return encoded_result(json.dumps(*args, **kwargs))

        def hashed(frame):
            self.metrics["codec:" + self.stage]["entry_hashed_octets"] += len(frame.json_bytes)
            return entry_hash(frame)

        real_open, real_connect, real_hash = builtins.open, sqlite3.connect, journal.hashlib.sha256

        def guard(call, *args, **kwargs):
            assert self.stage != "header", "header operation opened input or hashed source bytes"
            return call(*args, **kwargs)

        def authorized(kind):
            def check(action, table, column, *_rest):
                if action == sqlite3.SQLITE_READ:
                    if kind == "source" and self.stage == "header":
                        assert table == "frames" and column in {
                            "sequence",
                            "reference",
                            "prefix_state",
                        }
                    if kind == "corpus":
                        assert table != "assertion_payloads"
                        if self.stage == "identity":
                            assert table == "identities"
                return sqlite3.SQLITE_OK

            return check

        db.set_authorizer(authorized("corpus"))
        raw.set_authorizer(authorized("source"))
        # One callback per database sees every parent and child interval.
        db.set_progress_handler(progress("corpus"), 1)
        raw.set_progress_handler(progress("source"), 1)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(builtins, "open", lambda *a, **kw: guard(real_open, *a, **kw))
                patch.setattr(sqlite3, "connect", lambda *a, **kw: guard(real_connect, *a, **kw))
                patch.setattr(
                    journal.hashlib, "sha256", lambda *a, **kw: guard(real_hash, *a, **kw)
                )
                corpus_connection = Connection(db, self, "corpus")
                patch.setattr(workspace, "_db", corpus_connection)
                patch.setattr(workspace._corpus, "_db", corpus_connection)
                patch.setattr(
                    summary._store._storage, "connection", Connection(raw, self, "source")
                )
                patch.setattr(CanonicalCorpusValidator, "advance_source", algorithm)
                patch.setattr(retained, "_checkpoint", checkpointed)
                patch.setattr(retained, "_restore", restored_checkpoint)
                patch.setattr(journal_store, "json", SimpleNamespace(loads=loads))
                patch.setattr(
                    _Frames,
                    "__iter__",
                    lambda _: pytest.fail("raw source quantum enumerated its complete snapshot"),
                )
                patch.setattr(CanonicalCorpusValidator, "_identity", identified)
                patch.setattr(journal.JournalSummary, "anchor_at", anchored)
                patch.setattr(sha_owner, "sha256_compress", compressed)
                patch.setattr(sha_owner, "_rotr", rotated)
                patch.setattr(
                    sha_owner,
                    "json",
                    SimpleNamespace(
                        loads=loads,
                        dumps=dumped,
                        JSONDecodeError=json.JSONDecodeError,
                    ),
                )
                patch.setattr(workspace, "_register", registered)
                patch.setattr(source, "admit_source", admitted)
                patch.setattr(source, "verify_current", checked)
                patch.setattr(_Frames, "__getitem__", frame)
                patch.setattr(journal, "decode_document", decoded)
                patch.setattr(journal, "json", SimpleNamespace(loads=loads))
                patch.setattr(journal, "canonical_document", lambda v: encoded(header_encoded, v))
                patch.setattr(native, "canonical_document", lambda v: encoded(canonical, v))
                patch.setattr(
                    retained, "canonical_json_bytes", lambda v: encoded(parent_encoded, v)
                )
                patch.setattr(journal.JournalFrame, "sha256", property(hashed))
                yield
        finally:
            db.set_progress_handler(None, 0)
            raw.set_progress_handler(None, 0)
            db.set_authorizer(None)
            raw.set_authorizer(None)
        assert self.live == 0


def measure(
    workspace,
    source,
    summary,
    oracle,
    first,
    budget,
    monkeypatch,
    *,
    through=None,
    rejection=None,
    expected_reads=None,
):
    count = len(oracle) if through is None else through + 1
    stop = min(count, first + max(0, budget))
    selected = oracle[first:stop] if expected_reads is None else expected_reads
    assertions = sum(
        sum(len(v) for v in frame.document["body"].get("assertions", {}).values())
        for frame in selected
    )
    references = retractions = 0
    for frame in selected:
        retractions += len(frame.document["body"].get("retracts", ()))
        stack = [frame.document["body"].get("assertions", {})]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                if value.get("scope") == "external" and "assertion_id" in value:
                    references += 1
                else:
                    stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)
    db = workspace._db
    scalars = db.execute(
        "SELECT worker_epoch,version,sources,selections FROM corpus_workspace"
    ).fetchone()
    checkpoint = retained._checkpoint(workspace._corpus)
    owner_dimensions = {
        "binding_octets": len(source.binding.to_bytes()),
        "checkpoint_octets": len(checkpoint),
        "counter_octets": max(
            map(
                len,
                (
                    *scalars,
                    str(int(scalars[1]) + 1),
                    str(source.binding.source_count),
                    str(source.binding.selection_count),
                    str(db.execute("PRAGMA schema_version").fetchone()[0]),
                ),
            )
        ),
        "retained_rows": sum(
            db.execute("SELECT count(*) FROM " + table).fetchone()[0]
            for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        )
        + 10 * (len(selected) + assertions + references + retractions + 1),
    }
    native_dimensions = {
        "new_frames": len(selected),
        "frame_budget": max(0, budget),
        "new_assertions": assertions,
        "new_references": references,
        "new_retractions": retractions,
        "frame_octets": sum(len(frame.encoded) for frame in selected),
        "entry_octets": sum(len(frame.json_bytes) for frame in selected),
    }
    raw = summary._store._storage.connection
    header_rows = list(raw.execute("SELECT sequence,reference,prefix_state FROM frames"))
    sequence = count - 1
    _, reference, state = header_rows[sequence]
    sha = sha_owner.CheckpointSHA256.from_state(state)
    header_dimensions = {
        "retained_frames": len(oracle),
        "sequence_octets": len(str(sequence)),
        "header_octets": octets(reference) + octets(state),
        "padding_blocks": 1 if sha.total_bytes % 64 < 56 else 2,
    }
    metadata = max(
        max(octets(ref) + octets(state) for _, ref, state in header_rows),
        len(native.canonical_document(summary.anchor_at(sequence))),
    )
    observer = Observer()
    result = None
    with observer.observe(workspace, source, summary):
        if rejection is None:
            result = ROOT(workspace, summary, frame_budget=budget, through=through)
        else:
            with pytest.raises(rejection):
                ROOT(workspace, summary, frame_budget=budget, through=through)
    assert len(observer.frames) == len(selected)
    assert [index for index, _ in observer.frames] == list(range(first, first + len(selected)))
    coarse = defaultdict(int)
    coarse["admitted_frames"] = coarse["source_frame_reads"] = len(selected)
    coarse["admitted_frame_octets"] = coarse["source_frame_octets"] = native_dimensions[
        "frame_octets"
    ]
    coarse["hashed_entry_octets"] = observer.metrics["codec:native"]["entry_hashed_octets"]
    coarse["sql_statements"] = sum(
        observer.metrics[k]["sql_statements"] for k in ("corpus:native", "corpus:identity")
    )
    coarse["native_anchor_calls"] = len(observer.header_samples)
    coarse["native_identity_calls"] = len(observer.identity_samples)
    native_samples = []
    if observer.metrics["control"]["native_calls"]:
        native_samples.append(
            record(
                growth_binding(NATIVE),
                native_dimensions,
                coarse,
                {"anchor": observer.header_samples, "identity": observer.identity_samples},
            )
        )
    lifecycle = observer.metrics["corpus:owner"].copy()
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        lifecycle[metric] = observer.metrics["control"][metric]
    transaction = record(growth_binding(TRANSACTION), owner_dimensions, lifecycle)
    metrics = defaultdict(int)
    for key, values in observer.metrics.items():
        if key.startswith(("corpus:", "source:")):
            for metric, value in values.items():
                metrics[metric] += value
        elif key.startswith("codec:"):
            for metric in (
                "canonical_encoded_octets",
                "header_decoded_octets",
                "document_decoded_octets",
            ):
                metrics[metric] += values[metric]
    metrics.update(observer.metrics["control"])
    metrics["registration_statements"] = observer.metrics["corpus:registration"]["sql_statements"]
    metrics["parent_header_calls"] = len(observer.parent_headers)
    for metric in ("source_frame_reads", "source_frame_octets", "hashed_entry_octets"):
        metrics[metric] = coarse[metric]
    sample = record(
        growth_binding(ROOT),
        {
            **owner_dimensions,
            **native_dimensions,
            **header_dimensions,
            "native_calls": metrics["native_calls"],
            "parent_header_calls": metrics["parent_header_calls"],
            "source_metadata_octets": metadata,
        },
        metrics,
        {"native": native_samples, "transaction": [transaction], "header": observer.parent_headers},
    )
    for kind, query, parameters in observer.queries:
        if not query.startswith("SELECT"):
            continue
        connection = db if kind == "corpus" else raw
        plan = connection.execute("EXPLAIN QUERY PLAN " + query, parameters).fetchall()
        assert all("SCAN " not in row[-1] for row in plan), plan
    assert observer.live == 0 and observer.max_live <= 1
    return result, sample, observer


@pytest.mark.growth_witness(
    "native-retained-corpus-source-parent/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_source",
)
def test_source_parent_composes_actual_children_without_losing_outer_database_observation(
    tmp_path,
    journal,
    catalog,
    monkeypatch,
    record_property,
):
    samples = []
    for frames in (1, 17, 129):
        raw = history(journal, frames, catalog)
        oracle = journal_module_frames(raw)
        path = tmp_path / str(frames)
        path.mkdir()
        with validate_journal(raw, catalog=catalog) as summary:
            source = OwnedCorpusSource(summary, catalog)
            workspace = open_workspace(path, source, create=True, catalog=catalog)
            try:
                workspace._db.executemany(
                    "INSERT INTO identities VALUES (?,?,?)",
                    (
                        (f"unrelated-{i}", "entry", b"unrelated retained bytes" * 4096)
                        for i in range(129)
                    ),
                )
                workspace._db.commit()
                first = 0
                for call, budget in enumerate((1, 17, 129, 1, 1)):
                    result, sample, observed = measure(
                        workspace, source, summary, oracle, first, budget, monkeypatch
                    )
                    samples.append(sample)
                    first = min(frames, first + budget)
                    assert result.complete == (first == frames)
                    assert workspace._db.execute(
                        "SELECT sources FROM corpus_workspace"
                    ).fetchone() == ("1" if result.complete else "0",)
                    assert workspace._db.execute(
                        "SELECT count(*) FROM corpus_source_inputs"
                    ).fetchone() == (1 if result.complete else 0,)
                    assert (
                        sum(
                            v["metrics"]["sqlite_vm_instructions"]
                            for v in sample["delegates"]["native"][0]["delegates"]["identity"]
                        )
                        <= sample["metrics"]["sqlite_vm_instructions"]
                    )
                    if call == 0:
                        workspace.close()
                        workspace = open_workspace(path, source, epoch=1, catalog=catalog)
                assert all(v["metrics"]["source_frame_reads"] == 0 for v in samples[-2:])
            finally:
                workspace.close()
    record_property("growth_samples", samples)


def journal_module_frames(raw):
    from riverhog_provenance import parse_journal

    return parse_journal(raw)


@pytest.mark.growth_witness(
    "native-retained-corpus-source-rejection/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_source",
)
def test_source_parent_rejection_rolls_back_original_rows_unique_counts_and_checkpoint(
    tmp_path,
    journal,
    catalog,
    monkeypatch,
    record_property,
):
    samples = []
    oracle = journal_module_frames(journal)
    for scenario in ("manifest", "stale-before", "stale-after", "count", "budget"):
        path = tmp_path / scenario
        path.mkdir()
        with validate_journal(journal, catalog=catalog) as summary:
            source = OwnedCorpusSource(summary, catalog)
            if scenario == "count":
                source.binding = replace(source.binding, source_count=0)
            with open_workspace(path, source, create=True, catalog=catalog) as workspace:
                if scenario == "manifest":
                    source.sources = set()
                if scenario == "stale-before":
                    source.current = False
                budget = 0 if scenario == "budget" else 1
                expected = [] if scenario in ("manifest", "stale-before", "budget") else oracle
                error = ValueError if scenario == "budget" else IncompleteSourceError
                before = (path / "corpus.sqlite3").read_bytes()
                with monkeypatch.context() as patch:
                    if scenario == "stale-after":
                        checks = iter((True, False))
                        patch.setattr(source, "verify_current", lambda checks=checks: next(checks))
                    _, sample, _ = measure(
                        workspace,
                        source,
                        summary,
                        oracle,
                        0,
                        budget,
                        monkeypatch,
                        rejection=error,
                        expected_reads=expected,
                    )
                assert (path / "corpus.sqlite3").read_bytes() == before
                assert workspace._db.execute(
                    "SELECT count(*) FROM corpus_source_inputs"
                ).fetchone() == (0,)
                samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-source-prefixes/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_source",
)
def test_source_parent_keeps_exact_unique_prefixes_and_original_correction_rows(
    tmp_path, journal, who, catalog, monkeypatch, record_property
):
    from test_corpus_historical_source import correction_history

    raw, rows = correction_history(journal, who, catalog)
    oracle = journal_module_frames(raw)
    samples = []
    with validate_journal(raw, catalog=catalog) as summary:
        source = OwnedCorpusSource(summary, catalog)
        source.sources = {
            HistoryJournalAnchor.from_mapping(summary.anchor_at(sequence))
            for sequence in range(len(oracle))
        }
        source.binding = replace(
            source.binding,
            source_count=len(source.sources),
            source_manifest_sha256=source.digest(source.sources),
        )
        with open_workspace(tmp_path, source, create=True, catalog=catalog) as workspace:
            first = 0
            completed = set()
            for sequence in (1, 3, 5, 0, 2, 4, 5):
                result, sample, observed = measure(
                    workspace,
                    source,
                    summary,
                    oracle,
                    first,
                    len(oracle),
                    monkeypatch,
                    through=sequence,
                )
                assert result.complete and result.accepted_frames == sequence + 1
                first = max(first, sequence + 1)
                completed.add(sequence)
                assert sample["metrics"]["parent_header_calls"] == 1
                assert workspace._db.execute("SELECT sources FROM corpus_workspace").fetchone() == (
                    str(len(completed)),
                )
                assert workspace._db.execute(
                    "SELECT count(*) FROM corpus_source_inputs"
                ).fetchone() == (len(completed),)
                samples.append(sample)
            assert workspace._db.execute(
                "SELECT retired FROM assertions WHERE assertion=?", (rows[0]["assertion_id"],)
            ).fetchone() == ("2",)
            assert workspace._db.execute(
                "SELECT retired FROM assertions WHERE assertion=?", (rows[1]["assertion_id"],)
            ).fetchone() == ("5",)
            assert workspace._db.execute(
                "SELECT retired FROM assertions WHERE assertion=?", (rows[2]["assertion_id"],)
            ).fetchone() == (None,)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-retained-corpus-source-payloads/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_source",
)
def test_source_parent_charges_complete_original_payloads_and_external_references(
    tmp_path, journal, who, catalog, monkeypatch, record_property
):
    from riverhog_provenance import (
        BoundedSourceObserver,
        BytesSource,
        assertion,
        create_journal,
        external_reference,
        reference,
    )

    samples = []
    with validate_journal(journal, catalog=catalog) as primary:
        for count, width in ((1, 0), (17, 129), (129, 2048)):
            observed = BoundedSourceObserver(catalog=catalog).observe(
                BytesSource(b"source payload")
            )
            graph = observed.graph_fragment()
            graph["extensions"] = [
                assertion(
                    "extension",
                    who,
                    subject=reference(observed.artifact_id, "artifact"),
                    property="urn:test:source-payload",
                    value={"type": "text", "value": "ü" * width},
                )
                for _ in range(count)
            ]
            graph["relations"] = [
                assertion(
                    "derivation",
                    who,
                    used_state=external_reference(primary, primary.states[0]["id"]),
                    generated_state=reference(observed.state_id, "state"),
                    kind="copy",
                )
            ]
            raw = create_journal(graph, recorded_by_agent_id=who, catalog=catalog)
            oracle = journal_module_frames(raw)
            path = tmp_path / str(count)
            path.mkdir()
            with validate_journal(raw, catalog=catalog) as summary:
                source = OwnedCorpusSource(summary, catalog)
                with open_workspace(path, source, create=True, catalog=catalog) as workspace:
                    result, sample, observer = measure(
                        workspace, source, summary, oracle, 0, 1, monkeypatch
                    )
                    assert result.complete
                    assert sample["dimensions"]["new_references"] == 1
                    assert sample["metrics"]["source_frame_octets"] == len(raw)
                    assert sample["metrics"]["parameter_octets"] >= 2 * len(oracle[0].json_bytes)
                    assert workspace._db.execute("SELECT count(*) FROM refs").fetchone() == (1,)
                    samples.append(sample)
                    _, sample, observer = measure(
                        workspace, source, summary, oracle, 1, 1, monkeypatch
                    )
                    assert sample["metrics"]["source_frame_reads"] == 0
                    assert sample["metrics"]["returned_octets"] < len(raw)
                    samples.append(sample)
    record_property("growth_samples", samples)
