from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from contextlib import closing, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import replace
from functools import wraps

import pytest
import sha256_continuation as sha_owner
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    ProvenanceValidationError,
    assertion,
    create_journal,
    encode_entry,
    parse_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import journal as journal_owner
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._entry_replay import install as install_replay
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint, ValidationLedger
from riverhog_provenance._session_checkpoint import install as install_parent
from riverhog_provenance.journal_store import JournalStore
from riverhog_provenance_contracts import ENTRY_SCHEMA, ContractCatalog
from sha256_continuation import CheckpointSHA256
from test_child_admission_growth import octets
from test_entry_replay import ATTEMPT, owned_summary
from test_journal_prefix import checkpoint_frame
from test_semantic_edge import WorkConnection, observed_work
from test_semantic_owner import snapshot

# Prior history, historical payload, current payload, native kind, worker epoch, source width.
REGIMES = (
    (0, 1, 1, "init", 2, 64),
    (0, 1, 65537, "init", 2, 64),
    (1, 1, 1, "assertion", 2, 64),
    (17, 1, 1, "assertion", 2, 64),
    (129, 1, 1, "assertion", 2, 64),
    (1025, 1, 1, "assertion", 2, 64),
    (1, 65537, 1, "assertion", 2, 64),
    (1025, 65537, 1, "assertion", 2, 64),
    (17, 1, 65537, "assertion", 2, 64),
    (129, 1, 65537, "assertion", 2, 64),
    (17, 1, 1, "checkpoint", 2, 64),
    (17, 1, 1, "assertion", 2**200, 64),
    (0, 1, 1, "init", 2**200, 16384),
)


def owner(store, catalog, source_binding, epoch=2, **options):
    prefix = JournalPrefix(
        store.db,
        attempt_id=ATTEMPT,
        source_binding=source_binding,
        catalog=catalog,
        require_profiles=True,
        **options,
    )
    ledger = ValidationLedger(
        store.db, attempt_id=ATTEMPT, binding=prefix.binding, worker_epoch=epoch
    )
    return EntryReplay(store, prefix=prefix, ledger=ledger, catalog=catalog)


def finish_entry(current):
    for _ in range(100000):
        if current.ledger.load().checkpoint.phase == "frame":
            return
        current.advance(ReplayBudget(rows=64))
    pytest.fail("the finite native entry fixture did not complete")


@pytest.fixture(scope="session")
def admission_histories(tmp_path_factory, catalog):
    "Lazily retain real native semantic histories, never a fabricated accepted prefix."
    root = tmp_path_factory.mktemp("entry-admission-histories")
    observed = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"entry admission"))
    who = observed.observer_agent_id
    histories, saved, stores = {}, {}, []

    def first_frame(payload):
        fragment = deepcopy(observed.graph_fragment())
        fragment["extensions"] = [
            assertion(
                "extension",
                who,
                subject=reference(fragment["artifacts"][0]["id"], "artifact"),
                property="urn:test:admission-width",
                value={"type": "text", "value": "x" * payload},
            )
        ]
        return parse_journal(create_journal(fragment, recorded_by_agent_id=who, catalog=catalog))[0]

    def build(regime):
        count, historical_payload, payload, kind, epoch, source_width = regime
        key = historical_payload, source_width
        source_binding = "source:" + "x" * (source_width - 7)
        if key not in histories:
            store = JournalStore()
            stores.append(store)
            if source_width == 16384:
                # The existing 16 KiB limit covers the entire canonical parent
                # binding, including the source identity and mode envelope.
                probe = JournalPrefix(
                    store.db,
                    attempt_id=ATTEMPT,
                    source_binding="source:",
                    catalog=catalog,
                    require_profiles=True,
                )
                source_binding = "source:" + "x" * (source_width - len(probe.binding))
                probe = JournalPrefix(
                    store.db,
                    attempt_id=ATTEMPT,
                    source_binding=source_binding,
                    catalog=catalog,
                    require_profiles=True,
                )
                assert len(probe.binding) == source_width
            install_parent(store.db)
            install_replay(store.db)
            current = owner(store, catalog, source_binding)
            current.prefix.initialize()
            current.ledger.initialize()
            histories[key] = store, current, [], first_frame(historical_payload), source_binding
        store, current, frames, first, source_binding = histories[key]
        saved_key = key, count
        if saved_key not in saved:
            assert len(frames) <= count
            while len(frames) < count:
                frame = (
                    first if not frames else checkpoint_frame(current.prefix.load(), who, catalog)
                )
                current.admit(frame, ReplayBudget(rows=64))
                finish_entry(current)
                frames.append(frame)
            path = root / f"accepted-{len(saved)}.sqlite3"
            with closing(sqlite3.connect(path)) as db:
                store.db.backup(db)
            state = current.prefix.load()
            raw = b"".join(frame.encoded for frame in frames)
            assert state.frame_count == count and state.byte_count == len(raw)
            assert state.hexdigest == hashlib.sha256(raw).hexdigest()
            saved[saved_key] = path, raw, state
        path, raw, state = saved[saved_key]
        if not count:
            candidate = first_frame(payload)
        elif kind == "checkpoint":
            candidate = checkpoint_frame(state, who, catalog)
        else:
            target = first.document["body"]["assertions"]["artifacts"][0]["id"]
            row = assertion(
                "extension",
                who,
                subject=reference(target, "artifact"),
                property="urn:test:admission-current",
                value={"type": "text", "value": "x" * payload},
            )
            candidate = parse_journal(
                encode_entry(
                    journal_owner._entry(
                        journal_id=state.journal_id,
                        recorder_id=who,
                        kind="assertion",
                        body={"assertions": {"extensions": [row]}},
                        sequence=count,
                        previous=state.previous,
                    ),
                    catalog=catalog,
                )
            )[0]
        return {
            "path": path,
            "source_binding": source_binding,
            "epoch": epoch,
            "raw": raw + candidate.encoded,
            "candidate": candidate,
            "state": state,
            "who": who,
        }

    try:
        yield build
    finally:
        for store in stores:
            store.close()


class ReplayConnection(WorkConnection):
    def execute(self, sql, parameters=()):
        result = super().execute(sql, parameters)
        if self.recording:
            if sql.startswith("UPDATE journal_prefix"):
                self.metrics["published_metadata_octets"] += len(parameters[0].encode())
            elif sql.startswith("INSERT INTO frames"):
                self.metrics["published_payload_octets"] += len(parameters[1])
                self.metrics["published_metadata_octets"] += (
                    len(parameters[0].encode()) + len(parameters[2]) + len(parameters[3].encode())
                )
        return result

    @contextmanager
    def observe(self):
        with super().observe() as metrics:
            metrics.update(
                {
                    name: 0
                    for name in (
                        "source_hashed_octets",
                        "native_hashed_octets",
                        "sha256_compression_calls",
                        "decoded_json_octets",
                        "encoded_json_octets",
                        "native_schema_calls",
                        "published_metadata_octets",
                        "published_payload_octets",
                    )
                }
            )
            yield metrics

    def set_authorizer(self, callback):
        if callback is None:
            return super().set_authorizer(None)

        def authorize(action, table, column, database, trigger):
            if (
                action == sqlite3.SQLITE_READ
                and table == "journal_validation_frame"
                and column == "value"
            ):
                return sqlite3.SQLITE_DENY
            return callback(action, table, column, database, trigger)

        return super().set_authorizer(authorize)


@contextmanager
def observe_replay(
    db,
    catalog,
    dimensions,
    *,
    root_operation=EntryReplay.admit,
    input_dimensions=None,
    operation_owners=None,
    kernel_observer=None,
    delegate_selector=None,
    validation_policy=None,
    primitive_adapter=None,
    inclusive_operation=None,
    operation_aliases=None,
    failed_operations=(),
):
    binding = growth_binding(root_operation)
    projection = project_bindings((binding,))
    sample_hash = hashlib.sha256
    digests = {
        identity: sample_hash(canonical_json_bytes(value)).hexdigest()
        for identity, value in projection["declarations"].items()
    }

    def frame(owner, inputs):
        return {
            "binding": owner,
            "dimensions": inputs,
            "delegates": {name: [] for name, _operation in owner.delegates},
        }

    root = frame(binding, dimensions)
    stack, pending, operations = [root], [binding], set()
    while pending:
        parent = pending.pop()
        for _name, operation in parent.delegates:
            if operation not in operations:
                operations.add(operation)
                pending.append(growth_binding(operation))

    def complete(observed, primitive):
        metrics = dict(primitive)
        metrics.setdefault("metadata_octets", metrics.get("returned_metadata_octets", 0))
        for bound in observed["binding"].declaration.bounds:
            if not bound.metric.startswith(
                ("prefix_", "checkpoint_", "padding_", "coverage_", "edge_", "graph_")
            ):
                continue
            prefix, underlying = bound.metric.split("_", 1)
            selected = [
                name
                for name, operation in observed["binding"].delegates
                if (prefix == "prefix" and operation.__qualname__.startswith("JournalPrefix."))
                or (
                    prefix == "checkpoint"
                    and operation.__qualname__.startswith("ValidationLedger.")
                )
                or (prefix == "edge" and operation.__qualname__.startswith("SemanticEdge."))
                or (prefix == "graph" and operation.__qualname__.startswith("OnlineDag."))
                or (prefix == "padding" and operation.__name__ == "sha256_compress")
                or (
                    prefix == "coverage"
                    and operation.__qualname__.startswith("ObservationCoverage.")
                )
            ]
            metrics[bound.metric] = sum(
                child["metrics"][underlying]
                for name in selected
                for child in observed["delegates"][name]
            )
        declaration = observed["binding"].declaration
        return {
            "declaration": declaration.id,
            "declaration_sha256": digests[declaration.id],
            "dimensions": observed["dimensions"],
            "metrics": {bound.metric: metrics[bound.metric] for bound in declaration.bounds},
            "delegates": observed["delegates"],
        }

    class PeakBuffer(bytearray):
        def __init__(self, data):
            super().__init__(data)
            self.peak = len(self)

        def extend(self, data):
            super().extend(data)
            self.peak = max(self.peak, len(self))

    rotations = [0]
    real_rotate, real_native = sha_owner._rotr, hashlib.sha256
    real_loads, real_dumps, real_validate = json.loads, json.dumps, ContractCatalog.validate
    ledger_publication = ValidationLedger._publish
    replay_publication = EntryReplay._publish
    update_operation, compression_operation = (
        CheckpointSHA256.update_bytes,
        sha_owner.sha256_compress,
    )

    def rotate(value, shift):
        rotations[0] += 1
        return real_rotate(value, shift)

    class NativeHash:
        def __init__(self, current):
            self.current = current

        def __getattr__(self, name):
            return getattr(self.current, name)

        def update(self, data):
            if db.recording:
                db.metrics["native_hashed_octets"] += len(data)
            return self.current.update(data)

        def copy(self):
            return NativeHash(self.current.copy())

    def native(data=b"", **options):
        if db.recording:
            db.metrics["native_hashed_octets"] += len(data)
        return NativeHash(real_native(data, **options))

    def loads(data, *args, **kwargs):
        if db.recording:
            db.metrics["decoded_json_octets"] += len(
                data.encode() if isinstance(data, str) else data
            )
        return real_loads(data, *args, **kwargs)

    def dumps(data, *args, **kwargs):
        result = real_dumps(data, *args, **kwargs)
        if db.recording:
            db.metrics["encoded_json_octets"] += len(result.encode())
        return result

    def validate(current, schema_id, value):
        if db.recording:
            assert current is catalog
            if validation_policy is None:
                assert schema_id == ENTRY_SCHEMA
            else:
                validation_policy(schema_id, value)
            db.metrics["native_schema_calls"] += 1
        return real_validate(current, schema_id, value)

    def instrument(operation):
        @wraps(operation)
        def run(*args, **kwargs):
            if not db.recording:
                return operation(*args, **kwargs)
            parent = stack[-1]
            tags = [name for name, child in parent["binding"].delegates if child is operation]
            if operation is update_operation:
                data = args[1]
                db.metrics["source_hashed_octets"] += len(data)
                if not data:
                    return operation(*args, **kwargs)
                initial = len(args[0]._buffer)
                inputs = {
                    "input_octets": len(data),
                    "initial_tail_octets": initial,
                    "blocks": (initial + len(data)) // 64,
                }
                args[0]._buffer = PeakBuffer(args[0]._buffer)
            elif operation is compression_operation:
                db.metrics["sha256_compression_calls"] += 1
                if not tags:
                    # Checkpoint padding is the prefix owner's inclusive fixed overhead.
                    return operation(*args, **kwargs)
                inputs = {"block_octets": len(args[1])}
            elif (
                not tags
                and inclusive_operation is not None
                and inclusive_operation(parent["binding"], operation)
            ):
                # A globally observed method may also implement an inclusive leaf.
                # Keep its native work in that leaf without inventing a delegate call.
                return operation(*args, **kwargs)
            elif input_dimensions is not None:
                inputs = input_dimensions(parent["dimensions"], operation, args, kwargs)
            else:
                assert args[0].db is db
                inputs = {}
                for item in growth_binding(operation).declaration.dimensions:
                    ledger_rows = (
                        operation.__qualname__.startswith("ValidationLedger.")
                        and item.name == "retained_rows"
                    )
                    inputs[item.name] = parent["dimensions"][
                        "checkpoint_retained_rows" if ledger_rows else item.name
                    ]
                    if (
                        ledger_rows
                        and operation is ledger_publication
                        and root_operation is replay_publication
                    ):
                        inputs[item.name] += 1
            if len(tags) != 1 and delegate_selector is not None:
                tags = [delegate_selector(parent, operation, args, kwargs, tags)]
            assert len(tags) == 1
            observed = frame(growth_binding(operation), inputs)
            before, changes, start = dict(db.metrics), db.total_changes, len(db.statements)
            first_rotation = rotations[0]
            stack.append(observed)
            try:
                with (
                    kernel_observer(operation, args)
                    if kernel_observer is not None
                    else nullcontext(None)
                ) as kernel:
                    result = operation(*args, **kwargs)
            except ProvenanceValidationError:
                # Only explicitly selected primitive scopes have complete failure observations.
                if operation not in failed_operations or kernel is not None:
                    raise
                primitive = observed_work(db, before, changes, start)
                if primitive_adapter is not None:
                    primitive = primitive_adapter(operation, args, None, primitive)
                parent["delegates"][tags[0]].append(complete(observed, primitive))
                raise
            finally:
                assert stack.pop() is observed
            primitive = observed_work(db, before, changes, start)
            if kernel is not None:
                primitive = kernel(result)
            if operation is update_operation:
                calls = observed["delegates"]["compression"]
                primitive.update(
                    compression_calls=len(calls),
                    block_input_octets=sum(c["metrics"]["block_input_octets"] for c in calls),
                    rotate_calls=sum(c["metrics"]["rotate_calls"] for c in calls),
                    pending_buffer_octets=args[0]._buffer.peak,
                    retained_tail_octets=len(args[0]._buffer),
                )
            elif operation is compression_operation:
                primitive = {
                    "block_input_octets": len(args[1]),
                    "rotate_calls": rotations[0] - first_rotation,
                    "returned_words": len(result),
                }
            if primitive_adapter is not None:
                primitive = primitive_adapter(operation, args, result, primitive)
            parent["delegates"][tags[0]].append(complete(observed, primitive))
            return result

        return run

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sha_owner, "_rotr", rotate)
        patch.setattr(hashlib, "sha256", native)
        patch.setattr(json, "loads", loads)
        patch.setattr(json, "dumps", dumps)
        patch.setattr(ContractCatalog, "validate", validate)
        for operation in operations:
            operation_owner = (
                operation_owners[operation]
                if operation_owners is not None and operation in operation_owners
                else CheckpointSHA256
                if operation is update_operation
                else sha_owner
                if operation is compression_operation
                else JournalPrefix
                if operation.__qualname__.startswith("JournalPrefix.")
                else ValidationLedger
            )
            observed_operation = instrument(operation)
            patch.setattr(operation_owner, operation.__name__, observed_operation)
            if operation_aliases is not None:
                for alias_owner in operation_aliases.get(operation, ()):
                    patch.setattr(alias_owner, operation.__name__, observed_operation)
        yield root, complete
    assert stack == [root]


def admission_samples(tmp_path, catalog, build, *, samples, regimes=REGIMES):
    binding = growth_binding(EntryReplay.admit)
    projection = project_bindings((binding,))
    for regime in regimes:
        case = build(regime)
        candidate = case["candidate"]
        with validate_journal(case["raw"], catalog=catalog) as expected:
            expected_document = expected.materialize()
        with closing(JournalStore()) as store:
            path = store._storage.directory / "snapshot.sqlite3"
            store.db.close()
            shutil.copyfile(case["path"], path)
            store._storage.connection = sqlite3.connect(path)
            current = owner(store, catalog, case["source_binding"])
            if case["epoch"] != 2:
                ledger = current.ledger.replace_worker(case["epoch"])
                current = EntryReplay(store, prefix=current.prefix, ledger=ledger, catalog=catalog)
            before = current.ledger.load()
            after = replace(
                before.checkpoint,
                version=before.checkpoint.version + 1,
                phase="entry-admission",
                frame_sha256=candidate.sha256,
                frame_octets=len(candidate.encoded),
            )
            original = snapshot(store.db)
            width = 1024 + 2 * octets(
                (before, after, current.prefix.binding, str(case["epoch"]), candidate.reference)
            )
            dimensions = {
                "retained_rows": sum(len(rows) for rows in original.values()),
                "checkpoint_retained_rows": sum(
                    len(original[name])
                    for name in ("journal_validation", "journal_prefix", "frames")
                ),
                "metadata_row_octets": width,
                "retained_frames": case["state"].frame_count,
                "frame_octets": len(candidate.encoded),
                "initial_tail_octets": case["state"].byte_count % 64,
                "source_blocks": (case["state"].byte_count % 64 + len(candidate.encoded)) // 64,
            }
            # No setup reads enter the measured cold pager.
            store.db.close()
            store._storage.connection = sqlite3.connect(path, factory=ReplayConnection)
            store.db.execute("PRAGMA synchronous=FULL").close()
            store.db.execute("PRAGMA cache_size=-64").close()
            current = owner(store, catalog, case["source_binding"], case["epoch"])
            with observe_replay(store.db, catalog, dimensions) as (observed, complete):
                with store.db.observe():
                    admitted = current.admit(candidate, ReplayBudget(rows=64))
            primitive = observed_work(store.db, {key: 0 for key in store.db.metrics}, 0, 0)
            primitive["changed_rows"] = store.db.metrics["changed_rows"]
            sample = complete(observed, primitive)
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
            assert admitted.checkpoint == after and current.ledger.load() == admitted
            assert not store.db.in_transaction and current.prefix.load() == case["state"]
            changed = snapshot(store.db)
            assert {
                name: rows
                for name, rows in changed.items()
                if name not in {"journal_validation", "journal_validation_frame"}
            } == {
                name: rows
                for name, rows in original.items()
                if name not in {"journal_validation", "journal_validation_frame"}
            }
            assert changed["journal_validation_frame"] == [
                (1, str(case["state"].frame_count), candidate.sha256, candidate.json_bytes)
            ]
            with closing(sqlite3.connect(path)) as reader:
                assert snapshot(reader) == changed
            finish_entry(current)
            accepted = current.prefix.finish()
            with owned_summary(store, accepted) as actual:
                assert actual.materialize() == expected_document
                assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
                assert actual.journal_bytes == len(case["raw"])
    assert len(samples) == len(regimes)


@pytest.mark.growth_witness(
    "riverhog-entry-replay-admission-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay.admit",
)
def test_actual_cold_whole_entry_admission(tmp_path, catalog, admission_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    admission_samples(tmp_path, catalog, admission_histories, samples=samples)


def test_frame_and_parent_rollback_together_after_publication_failure(
    catalog, admission_histories, monkeypatch
):
    case = admission_histories(REGIMES[0])
    with closing(JournalStore()) as store:
        with closing(sqlite3.connect(case["path"])) as seed:
            seed.backup(store.db)
        current = owner(store, catalog, case["source_binding"])
        before = snapshot(store.db)
        real_publish = current.ledger._publish

        def fail_after_write(*args, **kwargs):
            real_publish(*args, **kwargs)
            raise StaleValidationCheckpoint("superseded before publication completed")

        with monkeypatch.context() as patch:
            patch.setattr(current.ledger, "_publish", fail_after_write)
            with pytest.raises(StaleValidationCheckpoint, match="superseded"):
                current.admit(case["candidate"])
        assert not store.db.in_transaction and snapshot(store.db) == before
        current.admit(case["candidate"])
        finish_entry(current)
        assert current.prefix.finish().hexdigest == hashlib.sha256(case["raw"]).hexdigest()
