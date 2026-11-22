"""Exact root custody, original independent readers and bounded lifetime work."""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import asdict
from functools import partial
from threading import Event, Thread

import pytest
import retained_resource
from riverhog_canonical_json import format_scalar
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalConstruction
from riverhog_provenance._generation_page_map import InvalidGenerationMap
from riverhog_provenance._generation_roots import GenerationRoots
from riverhog_provenance._generation_storage import NativeGenerationStorage
from riverhog_provenance._native_work import WorkAmount, WorkQuantum
from riverhog_provenance.errors import JournalValidationCapacity
from sqlite_admission import SqliteConnectionAdmission

from tests.support.growth_batch import SampleBatch
from tests.support.retained_resource import fault_native as fault_native


@pytest.fixture(autouse=True)
def native(monkeypatch, fault_native):
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)


@contextmanager
def storage(tmp_path, *, width=512, readers=4, metadata=512, create=True, on_close=None):
    directory = tmp_path / "owner"
    directory.mkdir(mode=0o700, exist_ok=True)
    admission = SqliteConnectionAdmission(
        connection_limit=1, cursor_limit=4, operation_limit=8, verify_process=lambda: None
    )
    frame = JournalConstruction(admission.connection_scope(), resource_limit=2)
    try:
        owner = NativeGenerationStorage.open(
            directory,
            construction=frame,
            page_size=width,
            reader_limit=readers,
            metadata_octets=metadata,
            create=create,
            on_storage_close=on_close,
        )
        yield owner, frame, admission
    finally:
        if frame.storage is not None and hasattr(frame.storage, "roots"):
            for reader in frame.storage.roots.readers:
                reader.close()
        assert frame.close_step()
        assert admission.status.connections == admission.status.cursors == 0


def apply(owner, operation, *, check=None, work=None, admission=None):
    with owner.execution(check, work=work):
        return owner.epoch.apply(
            lambda epoch: operation(), receive=lambda result: None, admission=admission
        )


def page(number, width=512):
    return number.to_bytes(8, "big") * (width // 8)


def build(owner, identity="grant:1", *, count=3, base=None, freeze=True):
    apply(owner, partial(owner.roots.begin, identity, base=base))
    root = None
    for number in range(1, count + 1):
        root = apply(
            owner,
            partial(owner.roots.replace, identity, number, page(number, owner.objects.page_size)),
        )
    if freeze:
        assert apply(owner, partial(owner.roots.freeze, identity)) == root
    return root


def scalar(db, query, values=()):
    with closing(db.execute(query, values)) as cursor:
        return cursor.fetchone()[0]


def collect(owner):
    while scalar(owner.db, "SELECT count(*) FROM generation_garbage"):
        apply(owner, partial(owner.objects.collect, limit=7))


@pytest.mark.parametrize("width", [512, 4096, 65536])
def test_row_retains_exact_old_root_through_new_mutations_and_actual_reader_receiving(
    tmp_path, width
):
    with storage(tmp_path, width=width) as (owner, _frame, _admission):
        first = build(owner)
        reader = owner.roots.borrow("grant:1", expected_root=first)
        build(owner, "grant:2", base="grant:1", count=0, freeze=False)
        changed = apply(owner, partial(owner.roots.replace, "grant:2", 2, page(99, width)))
        apply(owner, partial(owner.roots.freeze, "grant:2"))
        second = owner.roots.borrow("grant:2", expected_root=changed)
        collect(owner)
        assert reader.read(after=0, page_limit=4).pages == tuple(
            (n, page(n, width)) for n in range(1, 4)
        )
        assert second.read(after=1, page_limit=1).pages == ((2, page(99, width)),)
        statements = []
        owner.db.set_trace_callback(statements.append)
        try:
            with pytest.raises(RuntimeError, match="reader is not received"):
                owner.roots.retire("grant:1")
        finally:
            owner.db.set_trace_callback(None)
        assert not any(s.startswith("BEGIN") for s in statements)
        owner.epoch.require_quiescent()
        reader.close()
        owner.roots.retire("grant:1")
        collect(owner)
        assert second.read(after=0, page_limit=4).pages == (
            (1, page(1, width)),
            (2, page(99, width)),
            (3, page(3, width)),
        )
        second.close()
        owner.roots.retire("grant:2")
        collect(owner)
        assert scalar(owner.db, "SELECT count(*) FROM generation_objects") == 0


def test_parent_receiving_waits_for_actual_last_reader_and_callback_retry(tmp_path):
    calls = []
    current = []

    def receive():
        owner, frame, reader = current
        assert frame.dependents_received and owner.roots.dependents_received
        assert not reader.closed and not owner.storage_received
        calls.append(1)
        if len(calls) == 1:
            raise InterruptedError("original final receiving reply")

    with storage(tmp_path, on_close=receive) as (owner, frame, admission):
        root = build(owner)
        reader = owner.roots.borrow("grant:1", expected_root=root)
        current.extend((owner, frame, reader))
        assert not frame.close_step()
        assert not owner.storage_received and admission.status.connections == 1
        with pytest.raises(RuntimeError, match="closed"):
            owner.roots.borrow("grant:1", expected_root=root)
        assert reader.read(after=0, page_limit=1).pages == ((1, page(1)),)
        with pytest.raises(InterruptedError, match="original final"):
            reader.close()
        assert not reader.closed and owner.roots.readers == (reader,)
        assert not owner.storage_received and admission.status.connections == 0
        with pytest.raises(RuntimeError, match="closed"):
            reader.read(after=0, page_limit=1)
        reader.close()
        reader.close()
        assert reader.closed and owner.storage_received and calls == [1, 1]
        assert not owner.roots.readers and frame.close_step()


def test_reader_capacity_is_explicit_and_refuses_before_sql_or_shared_retain(tmp_path):
    with storage(tmp_path, readers=1) as (owner, _frame, _admission):
        root = build(owner)
        reader = owner.roots.borrow("grant:1", expected_root=root)
        before = asdict(owner.roots.work)
        with pytest.raises(JournalValidationCapacity, match="reader admission"):
            owner.roots.borrow("grant:1", expected_root=root)
        assert asdict(owner.roots.work) == before
        assert owner._storage._readers == 2 and owner.roots.readers == (reader,)
        reader.close()
        replacement = owner.roots.borrow("grant:1", expected_root=root)
        assert replacement is not reader


def test_original_reader_operation_blocks_concurrent_close_without_receiving_it(tmp_path):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner)
        reader = owner.roots.borrow("grant:1", expected_root=root)
        entered, release = Event(), Event()
        result, errors = [], []

        def check():
            entered.set()
            if not release.wait(timeout=5):
                raise RuntimeError("original test read did not receive release")

        def read():
            try:
                result.append(reader.read(after=0, page_limit=1, check=check))
            except BaseException as error:
                errors.append(error)

        thread = Thread(target=read)
        thread.start()
        try:
            assert entered.wait(timeout=5)
            with pytest.raises(RuntimeError, match="operation is active"):
                reader.close()
            assert not reader.closed and owner._storage._readers == 2
            assert not owner.roots.dependents_received
            with pytest.raises(RuntimeError, match="operation is active"):
                owner.roots.retire("grant:1")
        finally:
            release.set()
            thread.join(timeout=5)
        assert not thread.is_alive() and not errors
        assert result[0].pages == ((1, page(1)),)
        reader.close()
        owner.roots.retire("grant:1")


def test_reopen_retains_root_rows_with_arbitrary_precision_original_grant_identity(tmp_path):
    identity = "7:" + "a" * 64 + ":" + format_scalar("nonnegative", 1 << 1000)
    with storage(tmp_path, metadata=1024) as (owner, _frame, _admission):
        root = build(owner, identity)
        reader = owner.roots.borrow(identity, expected_root=root)
        reader.close()
        assert scalar(owner.db, "SELECT identity FROM generation_roots") == identity
    with storage(tmp_path, metadata=1024, create=False) as (owner, _frame, _admission):
        assert not owner.roots.readers
        collect(owner)
        reader = owner.roots.borrow(identity, expected_root=root)
        assert reader.read(after=0, page_limit=4).pages == tuple((n, page(n)) for n in range(1, 4))
        reader.close()
        owner.roots.retire(identity)
        collect(owner)
        assert scalar(owner.db, "SELECT count(*) FROM generation_objects") == 0


@pytest.mark.parametrize("damage", ["root", "root-type", "status"])
def test_corrupt_metadata_is_bounded_and_cannot_borrow_a_storage_lease(tmp_path, damage):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner)
        if damage == "root":
            owner.db.execute("UPDATE generation_roots SET root=?", ("a" * 1_000_000,)).close()
        elif damage == "root-type":
            owner.db.execute("UPDATE generation_roots SET root=zeroblob(1000000)").close()
        else:
            owner.db.execute("PRAGMA ignore_check_constraints=ON").close()
            owner.db.execute("UPDATE generation_roots SET status=?", ("x" * 1_000_000,)).close()
        owner.db.commit()
        before = owner.roots.work.loaded_octets
        with pytest.raises(InvalidGenerationMap):
            owner.roots.borrow("grant:1", expected_root=root)
        assert owner.roots.work.loaded_octets - before <= 75
        assert not owner.roots.readers and owner._storage._readers == 1


def test_failed_staging_keeps_previous_row_and_root_references_after_actual_rollback(tmp_path):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner, count=1)
        reader = owner.roots.borrow("grant:1", expected_root=root)
        build(owner, "grant:2", count=0, base="grant:1", freeze=False)
        original_refs = scalar(owner.db, "SELECT sum(refs) FROM generation_objects")
        wrote = []
        owner.db.set_trace_callback(
            lambda sql: (
                wrote.append(sql) if sql.startswith("UPDATE generation_roots SET root=") else None
            )
        )
        cause = InterruptedError("original post-stage check")

        def check():
            if wrote:
                raise cause

        try:
            with pytest.raises(InterruptedError) as caught:
                apply(owner, partial(owner.roots.replace, "grant:2", 2, page(2)), check=check)
            assert caught.value is cause
        finally:
            owner._storage.connection.set_trace_callback(None)
        assert (
            scalar(owner.db, "SELECT root FROM generation_roots WHERE identity=?", ("grant:2",))
            == root
        )
        assert reader.read(after=0, page_limit=2).pages == ((1, page(1)),)
        assert scalar(owner.db, "SELECT sum(refs) FROM generation_objects") == original_refs
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            owner.roots.borrow("grant:1", expected_root=root)


@pytest.mark.parametrize("failure", ["wrong-root", "missing-row", "mutable"])
def test_reader_never_substitutes_another_root_or_incomplete_generation(tmp_path, failure):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner, freeze=failure != "mutable")
        identity = "grant:missing" if failure == "missing-row" else "grant:1"
        with pytest.raises(InvalidGenerationMap):
            owner.roots.borrow(
                identity, expected_root="f" * 64 if failure == "wrong-root" else root
            )
        assert not owner.roots.readers and owner._storage._readers == 1


def test_immutable_root_cannot_be_mutated_even_without_readers(tmp_path):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner)
        with pytest.raises(InvalidGenerationMap, match="cannot change"):
            apply(owner, partial(owner.roots.replace, "grant:1", 1, page(99)))
        assert scalar(owner.db, "SELECT root FROM generation_roots") == root


def test_retirement_original_quantum_refusal_precedes_writer_and_is_retryable(tmp_path):
    with storage(tmp_path) as (owner, _frame, _admission):
        build(owner)
        quantum = WorkQuantum(WorkAmount(rows=3, octets=75, source_octets=1, transitions=1))
        statements = []
        owner.db.set_trace_callback(statements.append)
        try:
            with pytest.raises(JournalValidationCapacity):
                owner.roots.retire("grant:1", work=quantum)
        finally:
            owner.db.set_trace_callback(None)
        assert not any(s.startswith("BEGIN") for s in statements)
        assert quantum.held == WorkAmount()
        owner.roots.retire("grant:1")


def record(batch, owner, operation, action):
    before = asdict(owner.roots.work)
    statements = []
    owner.db.set_trace_callback(statements.append)
    try:
        result = action()
    finally:
        owner._storage.connection.set_trace_callback(None)
    metrics = {key: value - before[key] for key, value in asdict(owner.roots.work).items()}
    actual = [sql for sql in statements if "generation_roots" in sql]
    assert len(actual) == metrics["sql_calls"]
    sample = batch.sample(
        operation,
        {
            "units": 1,
            "metadata_octets": owner.roots.metadata_octets,
            "reader_limit": owner.roots.reader_limit,
        },
        metrics,
    )
    return sample, result


def lifecycle_samples(tmp_path, mode, numeric_policy):
    operation = getattr(GenerationRoots, mode)
    samples = []
    limits = (1, 4, 16) if mode == "retire" else (4,)
    with SampleBatch((operation,), limit=12, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            for readers in limits:
                case = tmp_path / f"w{width}-r{readers}"
                case.mkdir()
                with storage(case, width=width, readers=readers) as (owner, _frame, _admission):

                    def observe(action):
                        sample, result = record(batch, owner, operation, action)
                        samples.append(sample)
                        return result

                    if mode == "initialize":
                        owner.db.execute("DROP TABLE generation_roots").close()
                        owner.db.commit()
                        observe(partial(apply, owner, owner.roots.initialize))
                    elif mode == "begin":
                        observe(partial(apply, owner, partial(owner.roots.begin, "g:empty")))
                        build(owner, "g:base", count=1)
                        observe(
                            partial(
                                apply, owner, partial(owner.roots.begin, "g:next", base="g:base")
                            )
                        )
                    elif mode == "replace":
                        apply(owner, partial(owner.roots.begin, "g:1"))
                        for image in (page(1, width), page(2, width), None):
                            observe(
                                partial(apply, owner, partial(owner.roots.replace, "g:1", 1, image))
                            )
                    elif mode == "freeze":
                        build(owner, "g:1", count=1, freeze=False)
                        observe(partial(apply, owner, partial(owner.roots.freeze, "g:1")))
                    elif mode == "borrow":
                        root = build(owner, "g:1", count=1)
                        for _ in range(readers):
                            observe(partial(owner.roots.borrow, "g:1", expected_root=root))
                    else:
                        assert mode == "retire"
                        root = build(owner, "g:1", count=1)
                        held = [
                            owner.roots.borrow("g:1", expected_root=root) for _ in range(readers)
                        ]
                        apply(owner, partial(owner.roots.begin, "g:2", base="g:1"))
                        sample, _ = record(
                            batch, owner, operation, partial(owner.roots.retire, "g:2")
                        )
                        assert sample["metrics"]["reader_visits"] == readers
                        samples.append(sample)
                        for reader in held:
                            reader.close()
                        observe(partial(owner.roots.retire, "g:1"))
    return samples


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-initialize-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.initialize",
)
def test_actual_root_initialize_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "initialize", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-begin-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.begin",
)
def test_actual_root_begin_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "begin", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-replace-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.replace",
)
def test_actual_root_replace_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "replace", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-freeze-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.freeze",
)
def test_actual_root_freeze_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "freeze", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-borrow-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.borrow",
)
def test_actual_root_borrow_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "borrow", numeric_policy))


@pytest.mark.growth_witness(
    "riverhog-native-generation-root-retire-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_roots.GenerationRoots.retire",
)
def test_actual_root_retire_work(tmp_path, numeric_policy, record_property):
    record_property("growth_samples", lifecycle_samples(tmp_path, "retire", numeric_policy))


def test_growth_reader_rejects_extra_root_sql_work(tmp_path, numeric_policy):
    operation = GenerationRoots.borrow
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner)
        with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
            sample, reader = record(
                batch, owner, operation, partial(owner.roots.borrow, "grant:1", expected_root=root)
            )
        reader.close()
        invalid = {**sample, "metrics": {**sample["metrics"], "sql_calls": 2}}
        projection = project_bindings((growth_binding(operation),), numeric_policy=numeric_policy)
        with pytest.raises(ValueError, match="actual primitive observations violate"):
            validate_execution_samples(
                projection,
                growth_binding(operation).declaration.id,
                [invalid],
                numeric_policy=numeric_policy,
            )


@pytest.mark.parametrize("field", ["reader_limit", "metadata_octets"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.5])
def test_invalid_reader_or_metadata_admission_is_passive(tmp_path, field, value):
    directory = tmp_path / "owner"
    directory.mkdir(mode=0o700)
    calls = []
    admission = SqliteConnectionAdmission(
        connection_limit=1,
        cursor_limit=4,
        operation_limit=8,
        verify_process=lambda: calls.append("verify"),
    )
    frame = JournalConstruction(admission.connection_scope(), resource_limit=2)
    options = {"reader_limit": 4, "metadata_octets": 256, field: value}
    with pytest.raises(ValueError, match="positive original"):
        NativeGenerationStorage.open(
            directory, construction=frame, page_size=512, create=True, **options
        )
    assert not calls and not list(directory.iterdir())
    assert frame.close_step()


def test_borrow_original_final_check_failure_does_not_retain_a_hidden_lease(tmp_path):
    with storage(tmp_path) as (owner, _frame, _admission):
        root = build(owner)
        read = []
        owner.db.set_trace_callback(
            lambda sql: read.append(1) if sql.startswith("SELECT typeof(root)") else None
        )
        cause = KeyboardInterrupt("original post-metadata check")

        def check():
            if read:
                raise cause

        try:
            with pytest.raises(KeyboardInterrupt) as caught:
                owner.roots.borrow("grant:1", expected_root=root, check=check)
            assert caught.value is cause
        finally:
            owner.db.set_trace_callback(None)
        assert not owner.roots.readers and owner._storage._readers == 1
        reader = owner.roots.borrow("grant:1", expected_root=root)
        assert reader.read(after=0, page_limit=1).pages == ((1, page(1)),)
