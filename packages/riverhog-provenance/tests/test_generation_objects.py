"""Original SQLite epochs, immutable child custody and bounded reclamation."""

from __future__ import annotations

import hashlib
import struct
from contextlib import closing, contextmanager
from dataclasses import asdict
from functools import partial

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance._generation_objects import GenerationObjectStore
from riverhog_provenance._generation_page_map import GenerationPageMap, InvalidGenerationMap
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance._native_work import WorkAmount, WorkQuantum
from sqlite_admission import SqliteConnectionAdmission

from tests.support.growth_batch import SampleBatch


@contextmanager
def storage(*, width=512, initialize=True, database=":memory:"):
    admission = SqliteConnectionAdmission(
        connection_limit=1, cursor_limit=1, operation_limit=8, verify_process=lambda: None
    )
    scope = admission.connection_scope()
    db = scope.connect(database, cached_statements=0)
    owner = GenerationObjectStore(EpochContext(db), page_size=width)
    try:
        if initialize:
            apply(owner, owner.initialize)
        yield owner, admission
    finally:
        scope.close()
        assert scope.received
        assert admission.status.connections == 0
        assert admission.status.cursors == 0


def apply(owner, operation):
    return owner.epoch.apply(lambda epoch: operation(), receive=lambda result: None)


def page(number, width=512):
    return number.to_bytes(8, "big") * (width // 8)


def address(kind, body):
    return hashlib.sha256(f"riverhog-native-generation/{kind}/v1\0".encode() + body).hexdigest()


def build(owner, count):
    tree = GenerationPageMap(owner, page_size=owner.page_size)
    root = None
    for number in range(1, count + 1):
        previous = root

        def change(previous=previous, number=number):
            replacement = tree.replace(previous, number, page(number, owner.page_size))
            owner.retain(replacement)
            if previous is not None:
                owner.release(previous)
            return replacement

        root = apply(owner, change)
    return tree, root


def scalar(owner, sql, parameters=()):
    with closing(owner.db.execute(sql, parameters)) as cursor:
        return cursor.fetchone()[0]


def drain(owner, *, limit=7):
    total = 0
    while scalar(owner, "SELECT count(*) FROM generation_garbage"):
        before = owner.work.garbage_visits
        total += apply(owner, lambda: owner.collect(limit=limit))
        assert owner.work.garbage_visits - before <= limit
    return total


@pytest.mark.parametrize("width", [512, 4096, 65536])
def test_real_admitted_objects_keep_old_reader_roots_until_actual_release(width):
    with storage(width=width) as (owner, admission):
        tree, original = build(owner, 33)

        def replace():
            replacement = tree.replace(original, 16, page(999, width))
            owner.retain(replacement)
            return replacement

        changed = apply(owner, replace)
        drain(owner)
        assert tree.read(original, after=15, page_limit=1).pages == ((16, page(16, width)),)
        assert tree.read(changed, after=15, page_limit=1).pages == ((16, page(999, width)),)
        apply(owner, lambda: owner.release(original))
        assert drain(owner) == 9
        assert tree.read(changed, after=15, page_limit=1).pages == ((16, page(999, width)),)
        apply(owner, lambda: owner.release(changed))
        drain(owner)
        assert scalar(owner, "SELECT count(*) FROM generation_objects") == 0
        assert admission.status.cursors == 0


def test_reopened_storage_uses_exact_supplied_root_and_keeps_every_page(tmp_path):
    path = tmp_path / "owned.sqlite3"
    with storage(database=path) as (owner, _admission):
        _tree, root = build(owner, 65)
        drain(owner)
    with storage(database=path, initialize=False) as (owner, _admission):
        tree = GenerationPageMap(owner, page_size=512)
        actual = []
        after = 0
        while True:
            result = tree.read(root, after=after, page_limit=3)
            actual.extend(result.pages)
            if not result.more:
                break
            after = result.after
        assert actual == [(number, page(number)) for number in range(1, 66)]


def test_duplicate_objects_do_not_duplicate_child_references_or_hide_bad_content():
    with storage() as (owner, _admission):
        tree, root = build(owner, 16)
        original = scalar(owner, "SELECT sum(refs) FROM generation_objects")
        changed = apply(owner, lambda: tree.replace(root, 16, page(16)))
        assert changed == root
        assert scalar(owner, "SELECT sum(refs) FROM generation_objects") == original
        owner.db.execute(
            "UPDATE generation_objects SET body=? WHERE address=?", (b"wrong", root)
        ).close()
        owner.db.commit()
        with pytest.raises(InvalidGenerationMap, match="digest|extent|shape"):
            apply(owner, lambda: tree.replace(root, 16, page(16)))


@pytest.mark.parametrize("failure", ["missing-child", "wrong-child-kind", "edge-list", "raised"])
def test_original_failure_rolls_back_all_partial_interning_and_invalidates_epoch(failure):
    with storage() as (owner, _admission):
        body = page(1)
        child = address("page", body)
        original = RuntimeError("original object application failed")
        node = struct.pack(">BQ", 7, 1) + bytes.fromhex(child) + bytes(15 * 32)

        def fail():
            if failure != "missing-child":
                owner.store(child, "page", body, ())
            if failure == "wrong-child-kind":
                owner.db.execute(
                    "UPDATE generation_objects SET kind='node' WHERE address=?", (child,)
                ).close()
            owner.store(
                address("node", node), "node", node, () if failure == "edge-list" else (child,)
            )
            raise original

        with pytest.raises((InvalidGenerationMap, RuntimeError)) as caught:
            apply(owner, fail)
        if failure == "raised":
            assert caught.value is original
        assert scalar(owner, "SELECT count(*) FROM generation_objects") == 0
        assert scalar(owner, "SELECT count(*) FROM generation_garbage") == 0
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            apply(owner, owner.initialize)


def test_mutation_requires_original_epoch_and_positive_reference_receiving():
    with storage() as (owner, _admission):
        body = page(1)
        with pytest.raises(RuntimeError, match="original active"):
            owner.store(address("page", body), "page", body, ())
        _tree, root = build(owner, 1)
        apply(owner, partial(owner.release, root))
        with pytest.raises(InvalidGenerationMap, match="reference is unavailable"):
            apply(owner, partial(owner.release, root))


def test_bounded_blob_projection_refuses_oversize_before_materializing_it():
    with storage() as (owner, _admission):
        _tree, root = build(owner, 1)
        owner.db.execute(
            "UPDATE generation_objects SET body=zeroblob(1000000) WHERE address=?", (root,)
        ).close()
        owner.db.commit()
        before = owner.work.loaded_octets
        with pytest.raises(InvalidGenerationMap, match="admitted shape or extent"):
            owner.load(root, max_octets=521)
        assert owner.work.loaded_octets - before == 522


@pytest.mark.parametrize("damage", ["body", "missing", "negative", "real", "address"])
def test_collection_refuses_corrupt_original_dependencies_without_partial_receiving(damage):
    with storage() as (owner, _admission):
        _tree, root = build(owner, 1)
        drain(owner)
        apply(owner, partial(owner.release, root))
        if damage == "body":
            owner.db.execute(
                "UPDATE generation_objects SET body=? WHERE address=?", (b"x", root)
            ).close()
        elif damage == "missing":
            owner.db.execute("DELETE FROM generation_objects WHERE address=?", (root,)).close()
        elif damage == "negative":
            owner.db.execute("PRAGMA ignore_check_constraints=ON").close()
            owner.db.execute(
                "UPDATE generation_objects SET refs=-1 WHERE address=?", (root,)
            ).close()
        elif damage == "real":
            owner.db.execute(
                "UPDATE generation_objects SET refs=0.5 WHERE address=?", (root,)
            ).close()
        else:
            owner.db.execute("UPDATE generation_garbage SET address=?", ("x" * 1000,)).close()
        owner.db.commit()
        with pytest.raises(InvalidGenerationMap):
            apply(owner, lambda: owner.collect(limit=1))
        assert scalar(owner, "SELECT count(*) FROM generation_garbage") == 1


def test_cancellation_is_original_and_closes_every_cursor_before_rollback():
    with storage() as (owner, admission):
        _tree, root = build(owner, 16)
        original = InterruptedError("original cancellation")

        def cancel():
            raise original

        with pytest.raises(InterruptedError) as caught, owner.epoch.execution(cancel):
            apply(owner, partial(owner.release, root))
        assert caught.value is original
        assert admission.status.cursors == 0


def test_actual_bytes_and_sql_attempts_debit_the_original_work_quantum():
    with storage() as (owner, _admission):
        quantum = WorkQuantum(WorkAmount(rows=100, octets=10000, source_octets=1, transitions=1))
        before = asdict(owner.work)
        body = page(1)
        with owner.epoch.execution(None, work=quantum):
            owner.epoch.apply(
                lambda epoch: owner.store(address("page", body), "page", body, ()),
                receive=lambda result: None,
                admission=WorkAmount(rows=2, octets=2000),
            )
        assert quantum.consumed.rows == owner.work.sql_calls - before["sql_calls"] == 2
        assert quantum.consumed.octets == len(body) * 2 + len(
            b"riverhog-native-generation/page/v1\0"
        )
        assert quantum.held == WorkAmount()


def record(batch, owner, operation, action, *, units=1):
    before = asdict(owner.work)
    action()
    metrics = {name: value - before[name] for name, value in asdict(owner.work).items()}
    return batch.sample(
        operation, {"units": units, "object_octets": max(521, owner.page_size)}, metrics
    )


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-initialize-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.initialize",
)
def test_actual_original_object_schema_work(numeric_policy, record_property):
    operation = GenerationObjectStore.initialize
    samples = []
    with SampleBatch((operation,), limit=4, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            with storage(width=width, initialize=False) as (owner, _admission):
                samples.append(
                    record(batch, owner, operation, partial(apply, owner, owner.initialize))
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-load-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.load",
)
def test_actual_bounded_object_load_work(numeric_policy, record_property):
    operation = GenerationObjectStore.load
    samples = []
    with SampleBatch((operation,), limit=16, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            with storage(width=width) as (owner, _admission):
                for count in (1, 16, 65):
                    _tree, root = build(owner, count)
                    for value, maximum in ((root, 521), (address("page", page(1, width)), width)):
                        samples.append(
                            record(
                                batch,
                                owner,
                                operation,
                                partial(owner.load, value, max_octets=maximum),
                            )
                        )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-store-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.store",
)
def test_actual_object_interning_and_original_child_reference_work(numeric_policy, record_property):
    operation = GenerationObjectStore.store
    samples = []
    with SampleBatch((operation,), limit=16, numeric_policy=numeric_policy) as batch:
        for width in (512, 4096, 65536):
            with storage(width=width) as (owner, _admission):
                body = page(1, width)
                for _repeat in range(2):
                    samples.append(
                        record(
                            batch,
                            owner,
                            operation,
                            partial(
                                apply,
                                owner,
                                partial(owner.store, address("page", body), "page", body, ()),
                            ),
                        )
                    )
                child = address("page", body)
                children = (child,) * 16
                node = struct.pack(">BQ", 7, 16) + bytes.fromhex(child) * 16
                for _repeat in range(2):
                    samples.append(
                        record(
                            batch,
                            owner,
                            operation,
                            partial(
                                apply,
                                owner,
                                partial(owner.store, address("node", node), "node", node, children),
                            ),
                        )
                    )
                assert (
                    scalar(owner, "SELECT refs FROM generation_objects WHERE address=?", (child,))
                    == 16
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-retain-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.retain",
)
def test_actual_original_root_retention_work(numeric_policy, record_property):
    operation = GenerationObjectStore.retain
    samples = []
    with SampleBatch((operation,), limit=4, numeric_policy=numeric_policy) as batch:
        with storage() as (owner, _admission):
            for count in (1, 16, 65):
                _tree, root = build(owner, count)
                samples.append(
                    record(
                        batch, owner, operation, partial(apply, owner, partial(owner.retain, root))
                    )
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-release-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.release",
)
def test_actual_original_root_receiving_work(numeric_policy, record_property):
    operation = GenerationObjectStore.release
    samples = []
    with SampleBatch((operation,), limit=4, numeric_policy=numeric_policy) as batch:
        with storage() as (owner, _admission):
            for count in (1, 16, 65):
                _tree, root = build(owner, count)
                samples.append(
                    record(
                        batch, owner, operation, partial(apply, owner, partial(owner.release, root))
                    )
                )
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-native-generation-object-collect-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_objects.GenerationObjectStore.collect",
)
def test_actual_collection_work_follows_admitted_visits_and_actual_edges(
    numeric_policy, record_property
):
    operation = GenerationObjectStore.collect
    samples = []
    with SampleBatch((operation,), limit=16, numeric_policy=numeric_policy) as batch:
        for count in (1, 16, 65):
            for limit in (1, 4, 16):
                with storage() as (owner, _admission):
                    _tree, root = build(owner, count)
                    samples.append(
                        record(
                            batch,
                            owner,
                            operation,
                            partial(apply, owner, partial(owner.collect, limit=limit)),
                            units=limit,
                        )
                    )
                    apply(owner, partial(owner.release, root))
                    drain(owner, limit=limit)
                    assert scalar(owner, "SELECT count(*) FROM generation_objects") == 0
                    samples.append(
                        record(
                            batch,
                            owner,
                            operation,
                            partial(apply, owner, partial(owner.collect, limit=limit)),
                            units=limit,
                        )
                    )
    record_property("growth_samples", samples)


def test_growth_reader_rejects_an_extra_original_sql_attempt(numeric_policy):
    operation = GenerationObjectStore.load
    with storage() as (owner, _admission):
        _tree, root = build(owner, 1)
        with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
            sample = record(batch, owner, operation, lambda: owner.load(root, max_octets=521))
        invalid = {**sample, "metrics": {**sample["metrics"], "sql_calls": 2}}
        projection = project_bindings((growth_binding(operation),), numeric_policy=numeric_policy)
        with pytest.raises(ValueError, match="actual primitive observations violate"):
            validate_execution_samples(
                projection,
                growth_binding(operation).declaration.id,
                [invalid],
                numeric_policy=numeric_policy,
            )


def test_sql_and_hash_accounting_matches_independent_original_engine_calls(monkeypatch):
    from types import SimpleNamespace

    from riverhog_provenance import _generation_objects as object_owner

    with storage() as (owner, _admission):
        tree, root = build(owner, 16)
        body = page(999)
        digest = address("page", body)
        actual_hash = hashlib.sha256
        hashes, statements = [], []

        def hash_original(value):
            hashes.append(len(value))
            return actual_hash(value)

        monkeypatch.setattr(object_owner, "hashlib", SimpleNamespace(sha256=hash_original))
        owner.db.set_trace_callback(statements.append)
        try:
            for operation in (
                lambda: owner.load(root, max_octets=521),
                lambda: apply(owner, lambda: owner.store(digest, "page", body, ())),
                lambda: apply(owner, lambda: owner.store(digest, "page", body, ())),
                lambda: apply(owner, lambda: owner.collect(limit=16)),
            ):
                before = asdict(owner.work)
                sql_start, hash_start = len(statements), len(hashes)
                operation()
                observed = [
                    sql
                    for sql in statements[sql_start:]
                    if sql not in {"BEGIN IMMEDIATE", "COMMIT"}
                ]
                assert owner.work.sql_calls - before["sql_calls"] == len(observed)
                assert owner.work.hashed_octets - before["hashed_octets"] == sum(
                    hashes[hash_start:]
                )
                edges = sum(
                    sql.startswith("UPDATE generation_objects SET refs=") for sql in observed
                )
                assert owner.work.reference_updates - before["reference_updates"] == edges
        finally:
            owner.db.set_trace_callback(None)
        assert tree.read(root, after=0, page_limit=1).pages == ((1, page(1)),)


def test_positive_original_references_need_no_body_load_to_receive_stale_garbage():
    with storage() as (owner, _admission):
        tree, root = build(owner, 1)
        assert scalar(owner, "SELECT count(*) FROM generation_garbage") == 9
        before = asdict(owner.work)
        assert apply(owner, lambda: owner.collect(limit=9)) == 0
        assert owner.work.garbage_visits - before["garbage_visits"] == 9
        assert owner.work.loaded_octets - before["loaded_octets"] == 0
        assert owner.work.hashed_octets - before["hashed_octets"] == 0
        assert tree.read(root, after=0, page_limit=1).pages == ((1, page(1)),)


def test_refused_original_pre_writer_admission_has_no_storage_effect():
    from riverhog_provenance._native_work import NativeWorkCapacity

    with storage() as (owner, _admission):
        quantum = WorkQuantum(WorkAmount(rows=1, octets=2000, source_octets=1, transitions=1))
        original = asdict(owner.work)
        with owner.epoch.execution(None, work=quantum):
            with pytest.raises(NativeWorkCapacity) as caught:
                owner.epoch.apply(
                    lambda epoch: owner.store(address("page", page(1)), "page", page(1), ()),
                    receive=lambda result: None,
                    admission=WorkAmount(rows=2, octets=2000),
                )
            assert quantum.owns_admission_refusal(caught.value)
        assert owner.epoch.quiescent
        assert asdict(owner.work) == original
        assert scalar(owner, "SELECT count(*) FROM generation_objects") == 0


def test_failed_original_sql_cursor_admission_propagates_without_leaking_a_new_cursor():
    from sqlite_admission import SqliteCapacity

    with storage() as (owner, admission):
        held = owner.db.execute("SELECT 1")
        try:
            with pytest.raises(SqliteCapacity):
                apply(owner, lambda: owner.store(address("page", page(1)), "page", page(1), ()))
            assert admission.status.cursors == 1
        finally:
            held.close()
        assert scalar(owner, "SELECT count(*) FROM generation_objects") == 0
        assert admission.status.cursors == 0


def test_failed_child_receiving_rolls_back_the_entire_original_collection_quantum():
    with storage() as (owner, _admission):
        _tree, root = build(owner, 16)
        drain(owner)
        apply(owner, lambda: owner.release(root))
        owner.db.execute(
            "DELETE FROM generation_objects WHERE address=?", (address("page", page(16)),)
        ).close()
        owner.db.commit()

        def rows():
            with closing(
                owner.db.execute("SELECT address,refs FROM generation_objects ORDER BY address")
            ) as cursor:
                return cursor.fetchall()

        original = rows()
        with pytest.raises(InvalidGenerationMap, match="reference is unavailable"):
            apply(owner, lambda: owner.collect(limit=64))
        assert rows() == original
        assert scalar(owner, "SELECT count(*) FROM generation_garbage") == 1
