from __future__ import annotations

import hashlib
import sqlite3
import sys
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_replay as replay
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance import replay_binding as codec_owner
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from sqlalchemy import delete, event, insert, select, update

from tests.unit.test_journal_capture import Frame, retained
from tests.unit.test_journal_replay_binding import Scan, ready
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture
from tests.unit.test_staged_journal_range import EXTENT, JOURNAL, Chunk, Journal
from tests.unit.test_upload_journal_result_growth import history
from tests.unit.test_upload_journal_source_growth import octets, source_metadata_octets

PAGE = source_owner._JOURNAL_CAPTURE_SEGMENT_OCTETS


def source_snapshot(factory, work):
    with factory() as session:
        journal = session.get(Journal, (work.collection_id, work.journal_id))
        scan = session.get(Scan, (work.collection_id, work.journal_id))
        return (
            tuple(getattr(journal, column.key) for column in Journal.__table__.columns),
            None
            if scan is None
            else tuple(getattr(scan, column.key) for column in Scan.__table__.columns),
        )


@dataclass
class SourceCase:
    engine: object
    factory: object
    work: object
    raw: bytes
    spans: tuple[tuple[int, int], ...]
    journals: int
    binding_json: str | None
    atom: tuple[int, int] | None


@contextmanager
def build_cases(root, catalog, *, large=True):
    with ExitStack() as resources:
        cases = []
        specifications = [
            (1, 0, False, 1, False),
            (1, 65537, False, 1, False),
            (17, 0, False, 1, True),
            (1, 0, True, 1, False),
        ]
        if large:
            specifications.extend(
                (
                    (1025, 0, False, 1, False),
                    (1, 1024 * 1024, False, 1, False),
                    (1, 0, False, 1025, False),
                    (0, 0, False, 1, False),
                )
            )
        for index, (frames, payload, tiny_tail, journals, prepared) in enumerate(specifications):
            raw = (
                b"x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1)
                if frames == 0
                else history(source(payload, journal_id=JOURNAL), frames, catalog)
            )
            if tiny_tail:
                raw += b"malformed tail"
            engine, factory = resources.enter_context(retained(root / str(index), raw))
            work = ready(factory) if prepared else complete_capture(factory)
            binding_json = None
            if prepared:
                assert replay.prepare_upload_journal_replay(factory, work, catalog=catalog)
                binding_json = replay._binding(work, catalog).to_json()
            spans = tuple(
                (offset, min(len(raw), offset + EXTENT)) for offset in range(0, len(raw), EXTENT)
            )
            if tiny_tail:
                prefix = work.cursor.byte_offset
                assert 0 < prefix < len(raw) and work.phase == "deferred"
                spans = ((0, prefix), *((offset, offset + 1) for offset in range(prefix, len(raw))))
                with factory() as session, session.begin():
                    session.execute(delete(Chunk)).close()
                    session.execute(
                        insert(Chunk),
                        [
                            {
                                "collection_id": 1,
                                "journal_id": JOURNAL,
                                "ordinal": ordinal,
                                "byte_offset": start,
                                "content": raw[start:end],
                            }
                            for ordinal, (start, end) in enumerate(spans)
                        ],
                    ).close()
                    session.execute(update(Journal).values(next_chunk_ordinal=len(spans))).close()
            if journals > 1:
                with factory() as session, session.begin():
                    selected = session.get(Journal, (1, JOURNAL))
                    values = {
                        column.key: getattr(selected, column.key)
                        for column in Journal.__table__.columns
                    }
                    session.add_all(
                        [
                            Journal(
                                **{
                                    **values,
                                    "journal_id": (
                                        f"urn:uuid:{number:08x}-0000-4000-8000-000000000000"
                                    ),
                                }
                            )
                            for number in range(1, journals)
                        ]
                    )
            prefix = work.cursor.byte_offset
            with factory() as session:
                last = session.execute(
                    select(Frame.byte_offset, Frame.json_bytes)
                    .where(
                        Frame.collection_id == work.collection_id,
                        Frame.journal_id == work.journal_id,
                        Frame.directory_id == work.directory_id,
                    )
                    .order_by(Frame.ordinal.desc())
                    .limit(1)
                ).one_or_none()
            atom = None if last is None else (last.byte_offset + 1, last.json_bytes)
            cases.append(
                SourceCase(engine, factory, work, raw, spans, journals, binding_json, atom)
            )
        yield cases


@pytest.fixture(scope="module")
def source_cases(tmp_path_factory):
    catalog = source_owner.admission_provenance_catalog()
    with build_cases(tmp_path_factory.mktemp("source-port-inputs"), catalog) as cases:
        yield catalog, cases


class ReadRecorder:
    def __init__(self, case, operation, dimensions):
        self.case, self.operation, self.dimensions = case, operation, dimensions
        self.root_binding = growth_binding(operation)
        self.projection = project_bindings((self.root_binding,))
        self.bindings = {id(operation): self.root_binding}
        self.roles = {
            id(operation): "root",
            id(UploadJournalValidationSource.verify_current): "current",
            id(replay._probe): "probe",
            id(source_owner._read_upload_journal_capture_segment): "capture",
        }
        for function in (
            UploadJournalValidationSource.verify_current,
            replay._probe,
            source_owner._read_upload_journal_capture_segment,
        ):
            self.bindings[id(function)] = growth_binding(function)
        self.digests = {
            key: hashlib.sha256(canonical_json_bytes(value)).hexdigest()
            for key, value in self.projection["declarations"].items()
        }
        self.metrics = {
            name: 0
            for name in (
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "loaded_payload_octets",
                "payload_rows",
                "bound_parameter_octets",
                "sqlite_vm_instructions",
                "hashed_octets",
                "json_decodes",
                "json_encodes",
                "json_decoded_octets",
                "json_encoded_octets",
                "assembled_piece_octets",
            )
        }
        self.active, self.stack, self.statements, self.results = False, [], [], []
        self.actual_pages, self.transport_reads = [], []

    def row(self, value):
        if self.active and value is not None:
            self.metrics["returned_rows"] += 1
            values = (
                value
                if isinstance(value, (tuple, list)) or hasattr(value, "_mapping")
                else (value,)
            )
            blobs = [item for item in values if isinstance(item, bytes)]
            self.metrics["loaded_payload_octets"] += sum(map(len, blobs))
            self.metrics["payload_rows"] += int(bool(blobs))
            self.metrics["returned_metadata_octets"] += sum(
                octets(item) for item in values if not isinstance(item, bytes)
            )
        return value

    def invoke(self, function, tag, action):
        binding = self.bindings[id(function)]
        node = {
            "declaration": binding.declaration.id,
            "declaration_sha256": self.digests[binding.declaration.id],
            "delegates": {name: [] for name, _ in binding.delegates},
        }
        before = self.metrics.copy()
        self.stack.append(node)
        result = None
        try:
            result = action()
            return result
        finally:
            self.stack.pop()
            delta = {name: value - before[name] for name, value in self.metrics.items()}
            metrics = {
                bound.metric: delta.get(bound.metric, 0) for bound in binding.declaration.bounds
            }
            if self.roles[id(function)] == "probe":
                metrics.update(
                    source_payload_octets=delta["loaded_payload_octets"],
                    binding_octets=octets(None if result is None else result[1]),
                )
            elif self.roles[id(function)] == "current":
                metrics.update(
                    changed_rows=0,
                    source_payload_octets=delta["loaded_payload_octets"],
                    binding_octets=delta["returned_metadata_octets"],
                )
            elif self.roles[id(function)] == "capture":
                metrics.update(
                    source_rows=delta["payload_rows"],
                    loaded_segment_octets=delta["loaded_payload_octets"],
                    returned_octets=octets(result),
                )
            else:
                metrics.update(changed_rows=0, returned_octets=octets(result))
                for child_tag, records in node["delegates"].items():
                    for record in records:
                        for name, value in record["metrics"].items():
                            metrics[child_tag + "_" + name] += value
            node["metrics"] = metrics
            node["dimensions"] = {
                item.name: self.dimensions[item.name] for item in binding.declaration.dimensions
            }
            if self.stack:
                self.stack[-1]["delegates"][tag].append(node)
            else:
                self.sample = node

    @contextmanager
    def observe(self):
        recorder, case = self, self.case
        real_snapshot = replay.read_snapshot
        real_current = UploadJournalValidationSource.verify_current
        real_probe, real_capture = replay._probe, source_owner._read_upload_journal_capture_segment
        real_chunk = source_owner._upload_journal_chunk_at
        real_loads, real_encode = codec_owner.json.loads, codec_owner.canonical_json_bytes
        real_hash = source_owner.hashlib.sha256
        with case.engine.connect() as connection:
            native = connection.connection.driver_connection
            assert isinstance(native, sqlite3.Connection)
        changes = native.total_changes

        class Result:
            def __init__(self, actual):
                self.actual = actual
                recorder.results.append(self)

            def __enter__(self):
                self.actual.__enter__()
                return self

            def __exit__(self, *args):
                try:
                    return self.actual.__exit__(*args)
                finally:
                    assert self.actual.closed

            def __iter__(self):
                for row in self.actual:
                    yield recorder.row(row)

            def one_or_none(self):
                return recorder.row(self.actual.one_or_none())

        @contextmanager
        def snapshot(factory):
            with real_snapshot(factory) as session:
                execute, scalar = session.execute, session.scalar

                def observed_execute(*args, **kwargs):
                    return Result(execute(*args, **kwargs))

                def observed_scalar(*args, **kwargs):
                    return recorder.row(scalar(*args, **kwargs))

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(session, "execute", observed_execute)
                    patch.setattr(session, "scalar", observed_scalar)
                    yield session

        def sql(connection, _cursor, statement, parameters, _context, many):
            assert not many and connection.connection.driver_connection is native
            self.metrics["sql_statements"] += 1
            self.metrics["bound_parameter_octets"] += octets(parameters)
            self.statements.append((statement, parameters))

        def tick():
            self.metrics["sqlite_vm_instructions"] += 1
            return 0

        def authorize(action, table, column, _database, _trigger):
            if action == sqlite3.SQLITE_READ and column == "content":
                allowed = (
                    source_owner.CollectionUploadProvenanceCaptureSegmentRecord.__tablename__
                    if self.dimensions["captured_loads"]
                    else Chunk.__tablename__
                )
                if table != allowed or (
                    not self.dimensions["captured_loads"] and not self.dimensions["deferred_window"]
                ):
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        def loads(value, *args, **kwargs):
            self.metrics["json_decodes"] += 1
            self.metrics["json_decoded_octets"] += octets(value)
            return real_loads(value, *args, **kwargs)

        def encode(*args, **kwargs):
            self.metrics["json_encodes"] += 1
            value = real_encode(*args, **kwargs)
            self.metrics["json_encoded_octets"] += len(value)
            return value

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                recorder.metrics["hashed_octets"] += len(value)
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def sha256(value=b"", *args, **kwargs):
            self.metrics["hashed_octets"] += len(value)
            return Hash(real_hash(value, *args, **kwargs))

        def current(selected):
            return self.invoke(real_current, "current", lambda: real_current(selected))

        def probe(*args, **kwargs):
            return self.invoke(real_probe, "probe", lambda: real_probe(*args, **kwargs))

        def capture(*args, **kwargs):
            assert args[0] is case.factory and args[1] is case.work
            self.actual_pages.append(kwargs["ordinal"])
            return self.invoke(real_capture, "capture", lambda: real_capture(*args, **kwargs))

        def chunk(*args, **kwargs):
            result = real_chunk(*args, **kwargs)
            self.transport_reads.append((kwargs["offset"], result[0], result[1], len(result[2])))
            return result

        previous_profile = sys.getprofile()

        def profile(frame, event, arg):
            if previous_profile is not None:
                previous_profile(frame, event, arg)
            if event == "return" and frame.f_code is self.operation.__code__:
                self.metrics["assembled_piece_octets"] += sum(
                    map(len, frame.f_locals.get("pieces", ()))
                )

        event.listen(case.engine, "before_cursor_execute", sql)
        native.set_authorizer(authorize)
        native.set_progress_handler(tick, 1)
        self.active = True
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(replay, "read_snapshot", snapshot)
                patch.setattr(source_owner, "read_snapshot", snapshot)
                source_module = sys.modules[UploadJournalValidationSource.__module__]
                patch.setattr(source_module, "read_snapshot", snapshot)
                patch.setattr(UploadJournalValidationSource, "verify_current", current)
                patch.setattr(replay, "_probe", probe)
                patch.setattr(source_owner, "_read_upload_journal_capture_segment", capture)
                patch.setattr(source_owner, "_upload_journal_chunk_at", chunk)
                patch.setattr(codec_owner, "json", SimpleNamespace(loads=loads))
                patch.setattr(codec_owner, "canonical_json_bytes", encode)
                patch.setattr(source_owner, "hashlib", SimpleNamespace(sha256=sha256))
                sys.setprofile(profile)
                try:
                    yield
                finally:
                    sys.setprofile(previous_profile)
        finally:
            self.active = False
            native.set_progress_handler(None, 0)
            native.set_authorizer(None)
            event.remove(case.engine, "before_cursor_execute", sql)
        assert native.total_changes == changes
        assert case.engine.pool.checkedout() == 0 and all(
            item.actual.closed for item in self.results
        )
        for statement, parameters in self.statements:
            with case.engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as rows:
                    plan = " ".join(row[3] for row in rows)
            assert "SEARCH" in plan and "SCAN" not in plan and "USE TEMP" not in plan


def selected_pages(case, mode, offset, size, *, current=True):
    if not current or not size:
        return []
    prefix = case.work.cursor.byte_offset
    if mode == "read_window":
        return [offset // PAGE] if offset < prefix else []
    if size > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX or offset + size > prefix:
        return []
    return list(range(offset // PAGE, (offset + size - 1) // PAGE + 1))


def read_dimensions(case, selected, operation, offset, size, *, current=True):
    prefix = case.work.cursor.byte_offset
    valid = 0 <= offset <= len(case.raw) and 0 <= size <= len(case.raw) - offset
    pages = selected_pages(case, operation.__name__, offset, size, current=current) if valid else []
    if pages and selected._page is not None and selected._page[0] == pages[0]:
        pages = pages[1:]
    metadata = source_metadata_octets(case.work)
    raw = case.binding_json if current else None
    return {
        "retained_journals": case.journals,
        "binding_present": int(raw is not None),
        "binding_octets": octets(raw),
        "source_metadata_octets": metadata,
        "captured_segments": (prefix + PAGE - 1) // PAGE,
        "staged_chunks": len(case.spans),
        "range_octets": size,
        "current_calls": int(valid),
        "captured_loads": len(pages),
        "capture_control_octets": sum(metadata + octets(ordinal) for ordinal in pages),
        "deferred_window": int(
            valid
            and current
            and size > 0
            and operation.__name__ == "read_window"
            and offset >= prefix
        ),
    }


def expected_read(case, mode, offset, size, *, stale=False):
    if not (0 <= offset <= len(case.raw) and 0 <= size <= len(case.raw) - offset):
        return ValueError, None
    if stale:
        return source_owner._StaleUploadJournalValidation, None
    prefix = case.work.cursor.byte_offset
    if mode == "read_exact":
        if size > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            return JournalValidationCapacity, None
        if offset + size > prefix:
            return RuntimeError, None
        return None, case.raw[offset : offset + size]
    if not size:
        return None, b""
    if offset < prefix:
        end = min(offset + size, prefix, (offset // PAGE + 1) * PAGE)
    else:
        _, end = next((start, end) for start, end in case.spans if start <= offset < end)
        end = min(end, offset + size, offset + PAGE)
    return None, case.raw[offset:end]


def measured_read(catalog, case, operation, offset, size, cache, *, stale=False):
    selected = UploadJournalValidationSource(case.factory, case.work, catalog=catalog)
    pages = selected_pages(case, operation.__name__, offset, size)
    prefix_pages = (case.work.cursor.byte_offset + PAGE - 1) // PAGE
    if pages and cache != "cold":
        page = pages[0] if cache == "first" else (pages[0] + 1) % prefix_pages
        selected._captured_page(page)
    if stale:
        assert selected._page is not None
        with case.factory() as session, session.begin():
            session.execute(
                update(Journal)
                .where(Journal.collection_id == 1, Journal.journal_id == JOURNAL)
                .values(capture_version=case.work.version + 1)
            ).close()
    try:
        before = source_snapshot(case.factory, case.work)
        dimensions = read_dimensions(case, selected, operation, offset, size, current=not stale)
        expected_pages = selected_pages(case, operation.__name__, offset, size, current=not stale)
        if expected_pages and selected._page is not None and selected._page[0] == expected_pages[0]:
            expected_pages = expected_pages[1:]
        recorder = ReadRecorder(case, operation, dimensions)
        error, expected = expected_read(case, operation.__name__, offset, size, stale=stale)
        with recorder.observe():
            if error is None:
                result = recorder.invoke(
                    operation, "root", lambda: operation(selected, offset, size)
                )
                assert result == expected
            else:
                with pytest.raises(error):
                    recorder.invoke(operation, "root", lambda: operation(selected, offset, size))
        assert source_snapshot(case.factory, case.work) == before
        assert recorder.actual_pages == expected_pages
        if dimensions["deferred_window"]:
            ordinal, (start, end) = next(
                (ordinal, span)
                for ordinal, span in enumerate(case.spans)
                if span[0] <= offset < span[1]
            )
            assert recorder.transport_reads == [(offset, ordinal, start, end - start)]
        else:
            assert recorder.transport_reads == []
        validate_execution_samples(
            recorder.projection, recorder.root_binding.declaration.id, [recorder.sample]
        )
        return recorder.sample
    finally:
        if stale:
            with case.factory() as session, session.begin():
                session.execute(
                    update(Journal)
                    .where(Journal.collection_id == 1, Journal.journal_id == JOURNAL)
                    .values(capture_version=case.work.version)
                ).close()


def read_samples(catalog, cases, operation, *, samples):
    mode = operation.__name__
    for case in cases:
        prefix, total = case.work.cursor.byte_offset, len(case.raw)
        if mode == "read_window":
            offsets = {0, min(PAGE - 1, total), min(PAGE, total), total - 1, total}
            offsets.update((prefix - 1, prefix))
            vectors = {
                (offset, size)
                for offset in offsets
                if 0 <= offset <= total
                for size in {
                    0,
                    min(1, total - offset),
                    min(7, total - offset),
                    min(PAGE, total - offset),
                    total - offset,
                }
            }
        else:
            vectors = {(0, 0), (prefix, 0)}
            if case.atom is not None:
                start, size = case.atom
                vectors.update((start, count) for count in {0, 1, min(7, size), size})
                vectors.add((start + size - 1, 1))
            if prefix < total:
                vectors.add((prefix, 1))
            if total > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
                vectors.add((0, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1))
        for offset, size in sorted(vectors):
            pages = selected_pages(case, mode, offset, size)
            caches = (
                ("cold", "first", "other")
                if pages and (prefix + PAGE - 1) // PAGE > 1
                else ("cold", "first")
                if pages
                else ("cold",)
            )
            for cache in caches:
                samples.append(measured_read(catalog, case, operation, offset, size, cache))
        if case.atom is not None:
            start, _ = case.atom
            samples.append(measured_read(catalog, case, operation, start, 1, "first", stale=True))
    assert samples


@pytest.mark.growth_witness(
    "riverhog-retained-source-window-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_source.UploadJournalValidationSource.read_window",
)
def test_actual_captured_and_deferred_source_windows(source_cases, record_property):
    catalog, cases = source_cases
    samples = []
    record_property("growth_samples", samples)
    read_samples(catalog, cases, UploadJournalValidationSource.read_window, samples=samples)


@pytest.mark.growth_witness(
    "riverhog-retained-source-exact-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_source.UploadJournalValidationSource.read_exact",
)
def test_actual_captured_source_atoms(source_cases, record_property):
    catalog, cases = source_cases
    samples = []
    record_property("growth_samples", samples)
    read_samples(catalog, cases, UploadJournalValidationSource.read_exact, samples=samples)
