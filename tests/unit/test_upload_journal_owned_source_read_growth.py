from __future__ import annotations

import sys
from contextlib import ExitStack, contextmanager
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_certificates as certificates_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services import upload_journal_validation_owner as owner
from riverhog_core.services.upload_journal_source import UploadJournalValidationSource
from riverhog_growth_contracts import growth_binding, validate_execution_samples
from riverhog_provenance import InvalidJournalGraphCertificate, JournalValidationSession
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from sqlalchemy import select

from tests.unit.test_journal_capture import Frame, retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Chunk
from tests.unit.test_upload_journal_certificate_edge_growth import ready
from tests.unit.test_upload_journal_owned_source_growth import (
    APPLICATION,
    FUNCTIONS,
    Recorder,
    changed,
    input_dimensions,
    snapshot,
)
from tests.unit.test_upload_journal_source_ports_growth import (
    PAGE,
    expected_read,
    read_dimensions,
    selected_pages,
)
from tests.unit.test_upload_journal_validation_jobs_growth import build_cases as job_cases
from tests.unit.test_upload_journal_validation_jobs_growth import read_job, reset, selected

WINDOW, EXACT = owner._OwnedSource.read_window, owner._OwnedSource.read_exact
SOURCE_WINDOW, SOURCE_EXACT = (
    UploadJournalValidationSource.read_window,
    UploadJournalValidationSource.read_exact,
)
CAPTURE = source_owner._read_upload_journal_capture_segment
READ_FUNCTIONS = (WINDOW, EXACT, SOURCE_WINDOW, SOURCE_EXACT, CAPTURE)


@contextmanager
def build_cases(root, *, large=True):
    with ExitStack() as resources:
        cases = resources.enter_context(job_cases(root / "jobs", large=large))
        if large:
            catalog = source_owner.admission_provenance_catalog()
            for name, raw in (
                ("large-atom", source(1024 * 1024, journal_id=JOURNAL)),
                ("capacity", b"x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1)),
            ):
                path = root / name
                engine, factory = resources.enter_context(retained(path, raw))
                factory.configure(autoflush=False, expire_on_commit=False)
                work, certificates = ready(factory)
                journal_source = UploadJournalValidationSource(factory, work, catalog=catalog)
                opening = jobs.select_job(
                    factory,
                    work,
                    source_binding=journal_source.binding.to_json(),
                    certificate_binding=certificates.binding.to_json(),
                )
                assert opening is not None and opening.state == "opening"
                with owner.lease_journal_validation_workspace(
                    path / "native",
                    opening.workspace_id,
                    create=True,
                    creation_current=partial(jobs.creation_current, factory, work, opening),
                ) as directory:
                    assert directory is not None
                    claimed = jobs.claim_worker(factory, work, opening)
                    assert claimed is not None
                    owned = owner._OwnedSource(factory, journal_source, claimed, certificates)
                    native = JournalValidationSession.open_retained(
                        directory,
                        owned,
                        attempt_id=claimed.workspace_id,
                        worker_epoch=claimed.worker_epoch,
                        create=True,
                        catalog=catalog,
                        certificates=certificates,
                    )
                    try:
                        progress = native.progress()
                        assert progress.accepted_frames == 0
                        assert jobs.publish_progress(factory, work, claimed, progress) is not None
                    finally:
                        native.close()
                active = selected(factory)
                assert active is not None and active["state"] == "active"
                cases.append(
                    SimpleNamespace(
                        engine=engine,
                        factory=factory,
                        work=work,
                        active=active,
                        cleanup_ids={active["workspace_id"]},
                    )
                )
        for case in cases:
            with case.factory() as session:
                chunks = session.execute(
                    select(Chunk.byte_offset, Chunk.content)
                    .where(
                        Chunk.collection_id == case.work.collection_id,
                        Chunk.journal_id == case.work.journal_id,
                    )
                    .order_by(Chunk.ordinal)
                ).all()
                atom = session.execute(
                    select(Frame.byte_offset, Frame.json_bytes)
                    .where(
                        Frame.collection_id == case.work.collection_id,
                        Frame.journal_id == case.work.journal_id,
                        Frame.directory_id == case.work.directory_id,
                    )
                    .order_by(Frame.ordinal)
                    .limit(1)
                ).one_or_none()
                case.journals = session.query(certificates_owner.Journal).count()
                case.binding_json = session.scalar(
                    select(certificates_owner.Scan.replay_binding_json)
                )
            case.raw = b"".join(row.content for row in chunks)
            case.spans = tuple(
                (row.byte_offset, row.byte_offset + len(row.content)) for row in chunks
            )
            case.atom = None if atom is None else (atom.byte_offset + 1, atom.json_bytes)
            assert len(case.raw) == case.work.source_bytes
        yield cases


@pytest.fixture(scope="module")
def read_cases(tmp_path_factory):
    with build_cases(tmp_path_factory.mktemp("owned-source-read-inputs")) as cases:
        yield cases


class ReadRecorder(Recorder):
    def __init__(self, case, operation, inputs, dimensions):
        self.case, self.dimensions = case, dimensions
        self.actual_pages, self.transport_reads = [], []
        super().__init__(
            operation,
            inputs,
            functions=(*FUNCTIONS, *READ_FUNCTIONS),
            application=APPLICATION | {SOURCE_WINDOW, SOURCE_EXACT, CAPTURE},
            locations={
                WINDOW: (owner._OwnedSource, "read_window"),
                EXACT: (owner._OwnedSource, "read_exact"),
                SOURCE_WINDOW: (UploadJournalValidationSource, "read_window"),
                SOURCE_EXACT: (UploadJournalValidationSource, "read_exact"),
                CAPTURE: (source_owner, "_read_upload_journal_capture_segment"),
            },
            payload_operations={
                CAPTURE: {
                    source_owner.CollectionUploadProvenanceCaptureSegmentRecord.__tablename__
                },
                SOURCE_WINDOW: {Chunk.__tablename__},
            },
        )

    @contextmanager
    def observe(self, engine, *, cold):
        with super().observe(engine, cold=cold):
            actual_capture = source_owner._read_upload_journal_capture_segment
            actual_chunk = source_owner._upload_journal_chunk_at
            previous_profile = sys.getprofile()

            def capture(factory, work, *, ordinal):
                assert factory is self.case.factory and work is self.case.work
                self.actual_pages.append(ordinal)
                return actual_capture(factory, work, ordinal=ordinal)

            def chunk(*args, **kwargs):
                result = actual_chunk(*args, **kwargs)
                self.transport_reads.append(
                    (kwargs["offset"], result[0], result[1], len(result[2]))
                )
                return result

            def profile(frame, event, arg):
                if previous_profile is not None:
                    previous_profile(frame, event, arg)
                if event == "return" and frame.f_code is SOURCE_EXACT.__code__:
                    size = sum(map(len, frame.f_locals.get("pieces", ())))
                    self.full["assembled_piece_octets"] += size
                    self.application["assembled_piece_octets"] += size

            try:
                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(source_owner, "_read_upload_journal_capture_segment", capture)
                    patch.setattr(source_owner, "_upload_journal_chunk_at", chunk)
                    sys.setprofile(profile)
                    yield
            finally:
                sys.setprofile(previous_profile)


def measure(case, current, certificates, mode, offset, size, *, cold, kind=None):
    worker_good = kind not in {"worker", "worker-binding", "missing"}
    source_good = kind not in {"source", "binding", "replacement"}
    current.source_success = worker_good and source_good
    inputs, before = input_dimensions(case, current, certificates, worker_good, kind != "source")
    function = WINDOW if mode == "read_window" else EXACT
    source_function = SOURCE_WINDOW if mode == "read_window" else SOURCE_EXACT
    dimensions = read_dimensions(case, current.source, source_function, offset, size)
    dimensions["retained_journals"] = inputs[UploadJournalValidationSource.verify_current][
        "retained_journals"
    ]
    inputs[source_function] = dimensions
    inputs[CAPTURE] = {"captured_segments": dimensions["captured_segments"]}
    fields = {
        "metadata_row_octets": inputs[owner._OwnedSource.verify_current]["metadata_row_octets"]
    }
    for tag, child in owner._source_fence_children(mode):
        fields[tag + "_calls"] = (
            1 if tag == "verify" else int(current.source_success and kind != "manifest")
        )
        fields.update({tag + "_" + name: value for name, value in inputs[child].items()})
    inputs[function] = fields
    assert set(fields) == {item.name for item in growth_binding(function).declaration.dimensions}
    recorder = ReadRecorder(case, function, inputs, dimensions)
    error, expected = expected_read(case, mode, offset, size)
    if kind in {"worker", "worker-binding", "missing", "source"}:
        error = source_owner._StaleUploadJournalValidation
    elif kind in {"binding", "replacement"}:
        error = RuntimeError
    elif kind == "manifest":
        error = InvalidJournalGraphCertificate
    result, caught = None, None
    with recorder.observe(case.engine, cold=cold):
        try:
            result = getattr(current, mode)(offset, size)
        except BaseException as failure:
            caught = failure
    if error is None:
        if caught is not None:
            raise caught
        assert result == expected
    else:
        if caught is not None and not isinstance(caught, error):
            raise caught
        assert isinstance(caught, error)
    pages = selected_pages(case, mode, offset, size) if kind is None else []
    if pages and current.initial_page == pages[0]:
        pages = pages[1:]
    assert recorder.actual_pages == pages
    if kind is None and dimensions["deferred_window"]:
        ordinal, (start, end) = next(
            (ordinal, span)
            for ordinal, span in enumerate(case.spans)
            if span[0] <= offset < span[1]
        )
        assert recorder.transport_reads == [(offset, ordinal, start, end - start)]
    else:
        assert recorder.transport_reads == []
    assert snapshot(case.engine) == before
    validate_execution_samples(
        recorder.projection, growth_binding(function).declaration.id, [recorder.sample]
    )
    return recorder.sample


def read_samples(cases, mode, *, samples):
    for index, case in enumerate(cases):
        reset(case, case.active)
        catalog = source_owner.admission_provenance_catalog()
        certificates = certificates_owner.UploadJournalGraphCertificates(
            case.factory, case.work, catalog=catalog
        )
        selected_source = UploadJournalValidationSource(case.factory, case.work, catalog=catalog)
        current = owner._OwnedSource(
            case.factory, selected_source, read_job(case.active), certificates
        )
        prefix, total = case.work.cursor.byte_offset, len(case.raw)
        if mode == "read_window":
            vectors = {(0, 0), (0, min(7, total)), (0, total), (total, 0), (total + 1, 0)}
            vectors.update(
                (offset, min(7, total - offset)) for offset in (PAGE - 1, PAGE) if offset < total
            )
        else:
            vectors = {
                (0, 0),
                (0, min(7, total)),
                (0, min(prefix, PAGE + 7)),
                (prefix, 0),
                (total + 1, 0),
            }
            if case.atom is not None:
                vectors.add(case.atom)
            if total > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
                vectors.add((0, PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1))
        if prefix < total:
            vectors.add((prefix, 1))
        root = Path(case.engine.url.database).parent / "native"
        with owner.lease_journal_validation_workspace(root, current.job.workspace_id) as directory:
            assert directory is not None
            for offset, size in sorted(vectors):
                pages = selected_pages(case, mode, offset, size)
                caches = (None, pages[0]) if pages else (None,)
                if pages and (prefix + PAGE - 1) // PAGE > 1:
                    caches += ((pages[0] + 1) % ((prefix + PAGE - 1) // PAGE),)
                for cache in caches:
                    selected_source._page = None
                    if cache is not None:
                        selected_source._captured_page(cache)
                    current.initial_page = cache
                    for cold in (False, True) if (offset, size) == (0, min(7, total)) else (False,):
                        if cache is None:
                            selected_source._page = None
                        elif selected_source._page is None or selected_source._page[0] != cache:
                            selected_source._captured_page(cache)
                        samples.append(
                            measure(case, current, certificates, mode, offset, size, cold=cold)
                        )
            offset, size = case.atom if case.atom is not None else (0, min(1, total))
            size = min(size, 1)
            for kind in ("worker", "missing", "source", "binding", "manifest"):
                selected_source._page = None
                if prefix:
                    selected_source._captured_page(offset // PAGE)
                current.initial_page = (
                    None if selected_source._page is None else selected_source._page[0]
                )
                original = snapshot(case.engine)
                with changed(case, kind, certificates):
                    samples.append(
                        measure(
                            case,
                            current,
                            certificates,
                            mode,
                            offset,
                            size,
                            cold=bool(index % 2),
                            kind=kind,
                        )
                    )
                assert snapshot(case.engine) == original
        reset(case)


@pytest.mark.growth_witness(
    "riverhog-owned-source-window-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner._OwnedSource.read_window",
)
def test_actual_owned_source_windows_preserve_fences_and_source_port_costs(
    read_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    read_samples(read_cases, "read_window", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-owned-source-exact-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_owner._OwnedSource.read_exact",
)
def test_actual_owned_exact_reads_preserve_admission_and_assembly_costs(
    read_cases, record_property
):
    samples = []
    record_property("growth_samples", samples)
    read_samples(read_cases, "read_exact", samples=samples)
