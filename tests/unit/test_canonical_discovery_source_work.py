from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_extraction import scalar_postings
from riverhog_core.canonical_discovery_fact_work import (
    FactSegmentConflict,
    StaleFactTask,
    fact_extraction_cursor,
    fact_task_progress,
    finish_assertion,
)
from riverhog_core.canonical_discovery_source_work import (
    AdmittedAssertionSource,
    apply_source_page,
    prepare_lexical_source_page,
)

from tests.support.canonical_discovery_work import (
    KEY,
    ORIGIN,
    RecordedConnection,
    drive,
    open_work,
    source_for,
    streams,
    validated_source,
    write,
)


def scalar_projection(db):
    nodes = {key: json.loads(value) for key, value in streams(db, "path-node").items()}
    tokens = {key: value.decode() for key, value in streams(db, "path-token").items()}
    headers = {key: json.loads(value) for key, value in streams(db, "scalar-header").items()}
    values = streams(db, "scalar-value")
    result = []
    for key in sorted(values):
        parts = []
        current = key
        while nodes[current]["parent"] is not None:
            parts.append(tokens[current])
            current = nodes[current]["parent"]
        pointer = "".join("/" + part for part in reversed(parts))
        assert headers[key]["path_node"] == key
        assert headers[key]["representation"] == "json-scalar"
        assert headers[key]["source_value_sha256"] == hashlib.sha256(values[key]).hexdigest()
        result.append((pointer, json.loads(values[key]), headers[key]["source_value_sha256"]))
    return result


def oracle(row):
    return [
        (posting.pointer, posting.value, posting.source_value_sha256)
        for posting in scalar_postings(row)
    ]


@pytest.mark.parametrize("budget", [1, 7, 128])
def test_lexical_order_types_empty_paths_and_escaped_tokens_match_oracle(tmp_path, budget):
    row = {
        "assertion_id": "assertion",
        "tree": {"": [True, False, None, 17, "17", {}, []], "~//": {"\ue000": 1, "\U00010000": 2}},
        "type": "extension",
    }
    source = source_for(row)
    db = open_work(tmp_path / f"source-{budget}.sqlite3", source)
    try:
        drive(db, source, budget)
        assert scalar_projection(db) == oracle(row)
        assert streams(db, "canonical") == {0: canonical_json_bytes(row)}
        assert len(streams(db, "path-node")) == source.node_count
        assert streams(db, "path-token")[0] == b""
        assert source.locate("tree", "", 0).value is True
        assert (
            source.locate("tree", "~//", "\ue000").node
            < source.locate("tree", "~//", "\U00010000").node
        )
        progress, cursor = fact_extraction_cursor(db, KEY)
        assert cursor.category == "names" and cursor.stack_depth == 0
        assert not progress.open_streams
        with write(db), pytest.raises(FactSegmentConflict, match="unfinished"):
            finish_assertion(
                db, KEY, source_terminal=source.canonical_sha256, expected_version=progress.version
            )
    finally:
        db.close()


def test_long_utf8_value_is_encoded_and_hashed_once_while_fragments_span_pages(
    tmp_path, monkeypatch
):
    import riverhog_core.canonical_discovery_source_work as owner

    text = "long~/" + "\U0001f99b" * 12000
    row = {"assertion_id": "assertion", "value": text}
    source = source_for(row)
    db = open_work(tmp_path / "long.sqlite3", source)
    actual = owner.canonical_json_bytes
    encodes = []

    def observed(value):
        assert not db.in_transaction
        if value is text:
            encodes.append(value)
        return actual(value)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(owner, "canonical_json_bytes", observed)
            drive(db, source, 1)
        assert len(encodes) == 1
        assert scalar_projection(db) == oracle(row)
        node = source.locate("value").node
        assert json.loads(streams(db, "scalar-value")[node]) == text
        fragments = db.execute(
            "SELECT length(exact_bytes) FROM discovery_fact_segments "
            "WHERE build_id=? AND task_id=? AND kind='scalar-value' AND record_key=? "
            "ORDER BY ordinal",
            (KEY.build_id, KEY.task_id, str(node)),
        ).fetchall()
        assert len(fragments) > 10
        assert all(item == (4096,) for item in fragments[:-1])
    finally:
        db.close()


def test_deep_shared_prefix_is_stored_once_per_source_token_without_expanded_postings(tmp_path):
    subtree = {f"value-{i}": str(i) for i in range(50)}
    for _ in range(60):
        subtree = {"shared-long-token": subtree}
    row = {"assertion_id": "assertion", "tree": subtree}
    source = source_for(row)
    db = open_work(tmp_path / "deep.sqlite3", source)
    try:
        drive(db, source, 7)
        assert scalar_projection(db) == oracle(row)
        token_octets = sum(len(value) for value in streams(db, "path-token").values())
        expanded_octets = sum(len(posting.pointer.encode()) for posting in scalar_postings(row))
        assert expanded_octets > 20 * token_octets
        assert len(streams(db, "path-node")) == source.node_count
    finally:
        db.close()


def test_database_reopen_and_atom_reconstruction_continue_saved_physical_position(tmp_path):
    row = {
        "assertion_id": "assertion",
        "array": [{"~name/": "x" * 16000}, [1, 2, {"inner": "value"}]],
    }
    source = source_for(row)
    path = tmp_path / "reopen.sqlite3"
    db = open_work(path, source)
    reopenings = 0
    try:
        pages = 0
        while fact_extraction_cursor(db, KEY)[1].category in {"canonical", "scalars"}:
            page = prepare_lexical_source_page(db, KEY, source, action_budget=3)
            with write(db):
                progress = apply_source_page(db, KEY, page)
            pages += 1
            if pages in (1, 7, 17):
                saved = fact_extraction_cursor(db, KEY)
                db.close()
                source = source_for(row)
                db = open_work(path, source)
                assert fact_extraction_cursor(db, KEY) == saved
                assert progress == saved[0]
                reopenings += 1
        assert reopenings == 3
        assert scalar_projection(db) == oracle(row)
    finally:
        db.close()


def test_source_page_checks_exact_obligation_and_rejects_stale_replay(tmp_path):
    row = {"assertion_id": "assertion", "values": [1, 2, 3]}
    source = source_for(row)
    db = open_work(tmp_path / "fences.sqlite3", source)
    try:
        for changed in (
            source_for(row, "f" * 64),
            source_for({**row, "assertion_id": "other"}),
            source_for({**row, "values": [1, 2, 4]}),
            AdmittedAssertionSource("f" * 64, ORIGIN, row),
        ):
            with pytest.raises(StaleFactTask, match="obligation"):
                prepare_lexical_source_page(db, KEY, changed, action_budget=7)
        page = prepare_lexical_source_page(db, KEY, source, action_budget=7)
        with write(db):
            progress = apply_source_page(db, KEY, page)
        with write(db), pytest.raises(StaleFactTask, match="stale checkpoint"):
            apply_source_page(db, KEY, page)
        with write(db), pytest.raises(StaleFactTask):
            apply_source_page(db, replace(KEY, worker_id="other"), page)
        assert fact_task_progress(db, KEY) == progress
    finally:
        db.close()


def test_interruption_rolls_back_all_prepared_children_and_their_parent_cursor(tmp_path):
    source = source_for({"assertion_id": "assertion", "values": list(range(20))})
    db = open_work(tmp_path / "interrupted.sqlite3", source)
    try:
        before = fact_extraction_cursor(db, KEY)
        page = prepare_lexical_source_page(db, KEY, source, action_budget=128)
        db.executescript("""
            CREATE TRIGGER stop_page BEFORE UPDATE ON discovery_fact_cursors
            WHEN NEW.section_ordinal = 1
            BEGIN SELECT RAISE(ABORT, 'interrupted prepared page'); END;
        """)
        with write(db):
            with pytest.raises(sqlite3.IntegrityError, match="interrupted prepared page"):
                apply_source_page(db, KEY, page)
            assert fact_extraction_cursor(db, KEY) == before
            assert fact_task_progress(db, KEY).streams == 0
        db.execute("DROP TRIGGER stop_page")
        with write(db):
            apply_source_page(db, KEY, page)
        drive(db, source, 128)
        assert scalar_projection(db) == oracle(source.row)
    finally:
        db.close()


@pytest.mark.parametrize("budget", [0, -1, True, 1.0])
def test_invalid_operational_page_budget_does_not_change_source_work(tmp_path, budget):
    source = source_for({"assertion_id": "assertion"})
    db = open_work(tmp_path / "invalid.sqlite3", source)
    try:
        before = fact_extraction_cursor(db, KEY)
        with pytest.raises(ValueError):
            prepare_lexical_source_page(db, KEY, source, action_budget=budget)
        assert fact_extraction_cursor(db, KEY) == before
    finally:
        db.close()


def test_preparation_releases_reads_and_holds_no_owner_write_transaction(tmp_path):
    source = source_for({"assertion_id": "assertion"})
    db = open_work(tmp_path / "transactions.sqlite3", source)
    try:
        with write(db), pytest.raises(ValueError, match="outside"):
            prepare_lexical_source_page(db, KEY, source, action_budget=7)
        page = prepare_lexical_source_page(db, KEY, source, action_budget=7)
        assert not db.in_transaction
        with pytest.raises(ValueError, match="owner-managed"):
            apply_source_page(db, KEY, page)
    finally:
        db.close()


def test_valid_native_large_assertion_emits_all_fact_categories_through_prepared_pages(tmp_path):
    from riverhog_core.canonical_discovery_declared_source_work import (
        DeclaredAssertionSource,
        prepare_declared_source_page,
    )

    from tests.support.canonical_discovery import native_scalar_fixture

    source = validated_source(native_scalar_fixture(20_001))
    db = open_work(tmp_path / "native.sqlite3", source)
    try:
        pages = drive(db, source, 256)
        assert pages > 500
        actual = scalar_projection(db)
        assert actual == oracle(source.row)
        assert len(actual) > 20_001
        assert fact_extraction_cursor(db, KEY)[1].category == "names"
        assert streams(db, "canonical") == {0: canonical_json_bytes(source.row)}
        declared = DeclaredAssertionSource(source)
        assert declared.profile_count == declared.edge_count == 1
        while fact_extraction_cursor(db, KEY)[1].category != "done":
            page = prepare_declared_source_page(db, KEY, declared, action_budget=7)
            with write(db):
                apply_source_page(db, KEY, page)
        assert json.loads(streams(db, "profile-pin")[0]) == source.row["value"]["value"]["profile"]
        assert streams(db, "edge-role") == {0: b"subject"}
        progress, _ = fact_extraction_cursor(db, KEY)
        with write(db):
            assert finish_assertion(
                db, KEY, source_terminal=source.canonical_sha256, expected_version=progress.version
            ).complete
    finally:
        db.close()


def test_skipping_an_unvisited_child_cannot_pass_the_source_derived_terminal(tmp_path):
    source = source_for({"assertion_id": "assertion", "values": [1, 2, 3]})
    db = open_work(tmp_path / "missing-child.sqlite3", source)
    try:
        while True:
            page = prepare_lexical_source_page(db, KEY, source, action_budget=1)
            with write(db):
                apply_source_page(db, KEY, page)
            _, cursor = fact_extraction_cursor(db, KEY)
            if (
                cursor.category == "scalars"
                and cursor.section_ordinal == 5
                and cursor.stack_depth == 1
            ):
                break
        # Corrupt the saved root cursor to omit its first child. Traversal still
        # reaches the final source ordinal; the independently indexed source
        # totals must reject the missing streams at handoff.
        db.execute("UPDATE discovery_fact_stack SET next_child = 1 WHERE depth = 1")
        with pytest.raises(StaleFactTask, match="admitted source terminal"):
            drive(db, source, 7)
        assert fact_extraction_cursor(db, KEY)[1].category == "scalars"
    finally:
        db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-source-page-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_source_work.prepare_lexical_source_page",
)
def test_measured_source_pages_bound_sql_hashing_and_output_on_native_atoms(
    tmp_path, monkeypatch, record_property
):
    import riverhog_core.canonical_discovery_source_work as owner
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    from tests.support.canonical_discovery import native_profile_fixture

    binding = growth_binding(prepare_lexical_source_page)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_sha256 = hashlib.sha256
    deep = {"left": 1, "right": 2}
    for _ in range(48):
        deep = {"shared/token~": deep}
    samples = []
    for case, data, schema in (
        ("empty", [], {"type": "array", "items": {"type": "integer"}}),
        ("wide", list(range(65)), {"type": "array", "items": {"type": "integer"}}),
        ("long", "\U0001f99b" * 5000, {"type": "string"}),
        ("deep", deep, {"type": "object"}),
        ("documentary", None, None),
    ):
        admitted = validated_source(
            native_documentary_fixture()
            if case == "documentary"
            else native_profile_fixture(data, schema)
        )
        for budget in (1, 7, 128):
            source = source_for(admitted.row, admitted.origin_sha256)
            db = open_work(tmp_path / f"{case}-{budget}.sqlite3", source)
            try:
                page_number = 0
                deep_recorded = False
                while fact_extraction_cursor(db, KEY)[1].category in {"canonical", "scalars"}:
                    _, before = fact_extraction_cursor(db, KEY)
                    reference_step = (
                        before.category == "scalars"
                        and before.section_ordinal == 6
                        and source.documentary_reference(source.at_cursor(before)) is not None
                    )
                    selected = (
                        reference_step
                        or page_number in (0, 1, 7)
                        or (before.stack_depth >= 32 and not deep_recorded)
                    )
                    if selected:
                        deep_recorded = deep_recorded or before.stack_depth >= 32
                        recorder = RecordedConnection(db)

                        def observed_hash(value=b"", recorder=recorder, current_db=db, **kwargs):
                            assert not current_db.in_transaction
                            recorder.metrics["hashed_octets"] += len(value)
                            return actual_sha256(value, **kwargs)

                        with monkeypatch.context() as patch:
                            patch.setattr(owner.hashlib, "sha256", observed_hash)
                            page = prepare_lexical_source_page(
                                recorder, KEY, source, action_budget=budget
                            )
                        recorder.metrics["prepared_actions"] = len(page.actions)
                        recorder.metrics["prepared_fragment_octets"] = sum(
                            len(action.segment.exact_bytes)
                            for action in page.actions
                            if action.segment is not None
                        )
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": declaration_sha,
                            "dimensions": {
                                "action_budget": budget,
                                "atom_octets": len(source.canonical_json),
                            },
                            "metrics": dict(recorder.metrics),
                            "delegates": {},
                        }
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
                    else:
                        page = prepare_lexical_source_page(db, KEY, source, action_budget=budget)
                    with write(db):
                        apply_source_page(db, KEY, page)
                    page_number += 1
                assert scalar_projection(db) == oracle(admitted.row)
                assert len(streams(db, "documentary-reference")) == source.documentary_count
                if case == "documentary":
                    assert source.documentary_count == 1
                    assert list(streams(db, "documentary-reference").values()) == [
                        canonical_json_bytes(admitted.row["value"]["value"])
                    ]
            finally:
                db.close()
    record_property("growth_samples", samples)


def native_documentary_fixture():
    from types import SimpleNamespace

    from riverhog_provenance import (
        BoundedSourceObserver,
        BytesSource,
        assertion,
        create_journal,
        external_reference,
        reference,
        validate_journal,
    )

    from tests.support.canonical_discovery import native_scalar_fixture

    first = native_scalar_fixture(1)
    with validate_journal(first.raw, catalog=first.catalog) as native:
        selected = external_reference(native, native.states[0]["id"])
    observed = BoundedSourceObserver(catalog=first.catalog).observe(BytesSource(b"documentary"))
    row = assertion(
        "extension",
        observed.observer_agent_id,
        subject=reference(observed.artifact_id, "artifact"),
        property="urn:test:documentary-source",
        value={"type": "reference", "value": selected},
    )
    graph = observed.graph_fragment()
    graph["extensions"] = [row]
    raw = create_journal(
        graph, recorded_by_agent_id=observed.observer_agent_id, catalog=first.catalog
    )
    return SimpleNamespace(raw=raw, catalog=first.catalog, assertion_id=row["assertion_id"])


def documentary_tree():
    admitted = validated_source(native_documentary_fixture())
    selected = admitted.row["value"]["value"]
    row = {
        "assertion_id": admitted.assertion_id,
        "references": [selected, {"nested": selected}],
        "opaque": {
            "type": "json",
            "value": [selected, {**selected, "journal_id": "invalid opaque payload"}],
        },
    }
    return json.loads(canonical_json_bytes(row)), selected


@pytest.mark.parametrize("budget", [1, 7, 128])
def test_documentary_facts_keep_original_node_coordinates_without_changing_lexical_facts(
    tmp_path, budget
):
    row, reference = documentary_tree()
    source = source_for(row)
    db = open_work(tmp_path / "documentary.sqlite", source)
    drive(db, source, budget)
    expected = {
        source.locate("references", 0).node,
        source.locate("references", 1, "nested").node,
    }
    actual = streams(db, "documentary-reference")
    assert set(actual) == expected
    assert all(value == canonical_json_bytes(reference) for value in actual.values())
    assert source.documentary_count == len(actual) == 2
    assert fact_task_progress(db, KEY).streams == source.lexical_stream_count
    assert scalar_projection(db) == oracle(row)
    db.close()


def test_documentary_fragment_cursor_rolls_back_retries_and_reopens_without_duplicates(tmp_path):
    row, reference = documentary_tree()
    source = source_for(row)
    path = tmp_path / "documentary-reopen.sqlite"
    db = open_work(path, source)
    while True:
        page = prepare_lexical_source_page(db, KEY, source, action_budget=1)
        if page.actions[0].segment is not None:
            if page.actions[0].segment.stream.kind == "documentary-reference":
                break
        with write(db):
            apply_source_page(db, KEY, page)
    before = fact_extraction_cursor(db, KEY)
    db.execute(
        "CREATE TRIGGER fail_documentary BEFORE INSERT ON discovery_fact_segments "
        "WHEN NEW.kind='documentary-reference' "
        "BEGIN SELECT RAISE(ABORT,'injected documentary fragment failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected documentary"):
        apply_source_page(db, KEY, page)
    assert fact_extraction_cursor(db, KEY) == before
    db.execute("DROP TRIGGER fail_documentary")
    with write(db):
        apply_source_page(db, KEY, page)
    saved = fact_extraction_cursor(db, KEY)
    db.close()
    db = sqlite3.connect(path, isolation_level=None)
    source = source_for(row)
    assert fact_extraction_cursor(db, KEY) == saved
    drive(db, source, 1)
    assert len(streams(db, "documentary-reference")) == 2
    assert all(
        value == canonical_json_bytes(reference)
        for value in streams(db, "documentary-reference").values()
    )
    assert fact_task_progress(db, KEY).streams == source.lexical_stream_count
    db.close()
