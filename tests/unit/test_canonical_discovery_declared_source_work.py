from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_declared_source_work import (
    DeclaredAssertionSource,
    prepare_declared_source_page,
    prepare_declared_source_step,
)
from riverhog_core.canonical_discovery_extraction import (
    declared_profile_sections,
    name_postings,
)
from riverhog_core.canonical_discovery_fact_work import (
    StaleFactTask,
    fact_extraction_cursor,
    fact_task_progress,
    finish_assertion,
)
from riverhog_core.canonical_discovery_rows import CoreEdge, core_edges
from riverhog_core.canonical_discovery_source_work import apply_source_page
from riverhog_protocol import ProfilePin

from tests.support.canonical_discovery import native_profile_fixture
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

PIN = ProfilePin(
    contract_id="urn:test:declared-source",
    contract_sha256="a" * 64,
    schema_id="urn:test:declared-source:profile",
)


def byte_name(exact, encoding="utf-8"):
    return {
        "kind": "bytes",
        "bytes": {
            "encoding": "base64",
            "data": base64.b64encode(exact).decode("ascii"),
            "byte_length": str(len(exact)),
        },
        "encoding": encoding,
    }


def profile(data, pin=PIN):
    return {"profile": pin.model_dump(mode="json"), "data": data}


def reference(target="target", *, external=False):
    return {
        "scope": "external" if external else "local",
        "object_id": target,
        "object_type": "state",
        **(
            {
                "anchor": {
                    "journal_id": "foreign-journal",
                    "through": {"entry_id": "foreign-entry", "json_sha256": "b" * 64},
                }
            }
            if external
            else {}
        ),
    }


def paths(db):
    nodes = {key: json.loads(value) for key, value in streams(db, "path-node").items()}
    tokens = {key: value.decode() for key, value in streams(db, "path-token").items()}
    result = {}
    for node in sorted(nodes):
        parent = nodes[node]["parent"]
        result[node] = "" if parent is None else result[parent] + "/" + tokens[node]
    return result


def assert_projection(db, source):
    row = source.source.row
    pointers = paths(db)
    name_headers = streams(db, "name-header")
    name_values = streams(db, "name-value")
    assert sorted(name_headers) == sorted(name_values) == list(range(source.name_count))
    actual_names = []
    for ordinal in sorted(name_headers):
        header = json.loads(name_headers[ordinal])
        assert header["ordinal"] == ordinal
        actual_names.append(
            (
                pointers[header["path_node"]],
                json.loads(name_values[ordinal]),
                header["representation"],
                header["source_value_sha256"],
            )
        )
    assert actual_names == [
        (p.pointer, p.value, p.representation, p.source_value_sha256) for p in name_postings(row)
    ]
    headers = streams(db, "profile-header")
    pins = streams(db, "profile-pin")
    assert sorted(headers) == sorted(pins) == list(range(source.profile_count))
    actual_profiles = []
    for ordinal in sorted(headers):
        header = json.loads(headers[ordinal])
        assert header["ordinal"] == ordinal
        pointer = pointers[header["section_node"]]
        assert pointers[header["data_node"]] == pointer + "/data"
        actual_profiles.append((pointer, json.loads(pins[ordinal])))
    assert actual_profiles == [
        (section.pointer, section.profile.model_dump(mode="json"))
        for section in declared_profile_sections(row)
    ]
    headers = streams(db, "edge-header")
    roles, targets, foreign = (
        streams(db, "edge-" + kind) for kind in ("role", "target", "foreign")
    )
    assert (
        sorted(headers)
        == sorted(roles)
        == sorted(targets)
        == sorted(foreign)
        == list(range(source.edge_count))
    )
    actual_edges = []
    for ordinal in sorted(headers):
        header = json.loads(headers[ordinal])
        assert header["ordinal"] == ordinal
        assert header["path_node"] in pointers
        actual_edges.append(
            CoreEdge(
                roles[ordinal].decode(),
                targets[ordinal].decode(),
                header["target_type"],
                header["target_scope"],
                foreign[ordinal] or None,
            )
        )
    assert tuple(actual_edges) == core_edges(row)
    progress, cursor = fact_extraction_cursor(db, KEY)
    assert cursor.category == "done"
    assert not progress.open_streams
    assert progress.streams == dict(source.category_stream_totals)["done"]


def drive_declared(db, source, budget):
    pages = 0
    while fact_extraction_cursor(db, KEY)[1].category != "done":
        page = prepare_declared_source_page(db, KEY, source, action_budget=budget)
        assert 1 <= len(page.actions) <= budget
        assert sum(len(a.segment.exact_bytes) for a in page.actions if a.segment) <= 4096 * budget
        with write(db):
            progress = apply_source_page(db, KEY, page)
        assert progress.version == page.expected_version + len(page.actions)
        pages += 1
    return pages


def complete(tmp_path, row, budget=7):
    atom = source_for(row)
    db = open_work(tmp_path / "facts.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        drive_declared(db, source, budget)
        assert_projection(db, source)
        progress, _ = fact_extraction_cursor(db, KEY)
        with write(db):
            finished = finish_assertion(
                db, KEY, source_terminal=atom.canonical_sha256, expected_version=progress.version
            )
        assert finished.complete
    finally:
        db.close()


@pytest.mark.parametrize("budget", [1, 7, 128])
def test_declared_name_grammar_preserves_record_order_exact_bytes_and_optional_text(
    tmp_path, budget
):
    names = [
        byte_name(b""),
        byte_name(b"A\\\x00\xff"),
        byte_name("declared \U0001f99b".encode()),
        byte_name("LE \U0001f99b".encode("utf-16le"), "utf-16le"),
        byte_name(b"\x00", "utf-16le"),
        byte_name(b"opaque", "unspecified"),
        {"kind": "text", "value": "ordinary lexical text"},
    ]
    row = {
        "assertion_id": "assertion",
        "type": "observation",
        "evidence": [{"source_identifier": {"value": value}} for value in names],
        "identifiers": [{"value": byte_name(b"identifier")}],
        "consistency": {"snapshot_identifier": {"value": byte_name(b"snapshot")}},
        "metadata": [
            {"name": byte_name(b"first"), "source": {"field": byte_name(b"first-field")}},
            {"name": byte_name(b"second"), "source": {"field": byte_name(b"second-field")}},
            {"name": {"kind": "text", "value": "ordinary lexical text"}, "source": {}},
        ],
        "profiles": [profile({"name": byte_name(b"opaque fake name"), "state": reference()})],
    }
    complete(tmp_path, row, budget)


@pytest.mark.parametrize(
    "row",
    [
        {"type": "delivery_association", "slot": byte_name(b"slot")},
        {
            "type": "locator_binding",
            "locator": {"kind": "filesystem_path", "name": byte_name(b"path")},
        },
        {"type": "locator_binding", "locator": {"kind": "object_key", "key": byte_name(b"key")}},
        {
            "type": "locator_binding",
            "locator": {
                "kind": "object_key",
                "key": byte_name(b"key"),
                "version": byte_name(b"version"),
            },
        },
        {
            "type": "locator_binding",
            "locator": {
                "kind": "repository_id",
                "identifier": byte_name(b"id"),
                "version": byte_name(b"version"),
            },
        },
        {"type": "locator_binding", "locator": {"kind": "opaque", "value": byte_name(b"opaque")}},
    ],
)
def test_kind_specific_name_slots_match_existing_projection(tmp_path, row):
    complete(tmp_path, {"assertion_id": "assertion", **row})


@pytest.mark.parametrize(
    "row",
    [
        {"type": "extension", "value": {"type": "json", "value": profile({"empty": []})}},
        {"type": "context", "profiles": [profile({}), profile({"second": True})]},
        {"type": "observation", "profiles": [profile([]), profile("second")]},
        {"type": "reported_description", "profiles": [profile(17)]},
        {"type": "activity", "details": [profile({}), profile({})], "configuration": profile({})},
        {
            "type": "locator_binding",
            "locator": {"kind": "database_selector", "selector": profile({"query": "opaque"})},
        },
    ],
)
def test_complete_profile_sections_keep_source_identity_and_ignore_nested_lookalikes(tmp_path, row):
    complete(tmp_path, {"assertion_id": "assertion", **row})


@pytest.mark.parametrize(
    "row",
    [
        {"type": "occurrence", "artifact_id": "artifact", "source_context_id": "context"},
        {"type": "state", "occurrence_id": "occurrence"},
        {
            "type": "observation",
            "capture_id": "capture",
            "source_context_id": "context",
            "state": reference(),
        },
        {
            "type": "reported_description",
            "source_context_id": "context",
            "state": reference(external=True),
        },
        {
            "type": "locator_binding",
            "locator": {"kind": "opaque", "value": {"kind": "text"}},
            "context_id": "context",
            "observation_id": "observation",
            "target": reference(),
        },
        {"type": "usage", "activity_id": "activity", "state": reference()},
        {"type": "generation", "activity_id": "activity", "state": reference()},
        {"type": "invalidation", "activity_id": "activity", "state": reference()},
        {
            "type": "derivation",
            "activity_id": "activity",
            "usage_id": "usage",
            "generation_id": "generation",
            "used_state": reference("used"),
            "generated_state": reference("generated", external=True),
        },
        {
            "type": "specialization",
            "specific": reference("specific"),
            "general": reference("general"),
        },
        {"type": "alternate", "left": reference("left"), "right": reference("right")},
        {
            "type": "delivery_association",
            "delivery_context_id": "context",
            "verification_observation_id": "observation",
            "state": reference(),
        },
        {
            "type": "custody_assertion",
            "custodian_agent_id": "agent",
            "context_id": "context",
            "target": reference(),
        },
        {"type": "journal_subject", "journal_id": "journal", "artifact": reference()},
        {
            "type": "extension",
            "subject": reference("subject"),
            "value": {"type": "reference", "value": reference("value", external=True)},
        },
        {
            "type": "activity",
            "associations": [{"agent_id": "agent"}],
            "contexts": [{"role": "role", "context_id": "context"}],
        },
        {"type": "context"},
    ],
)
def test_all_core_edge_kinds_keep_order_scope_and_exact_foreign_preimage(tmp_path, row):
    complete(tmp_path, {"assertion_id": "assertion", **row})


def test_process_replacement_resumes_inside_each_fragmented_declared_category(tmp_path):
    pin = ProfilePin(
        contract_id="urn:test:" + "\U0001f99b" * 1000,
        contract_sha256=PIN.contract_sha256,
        schema_id="urn:test:" + "\U0001f99b" * 1000,
    )
    row = {
        "assertion_id": "assertion",
        "type": "activity",
        "evidence": [{"source_identifier": {"value": byte_name("\U0001f99b".encode() * 4000)}}],
        "details": [profile({}, pin)],
        "contexts": [{"role": "long/" + "\U0001f99b" * 4000, "context_id": "context"}],
        "associations": [{"agent_id": f"agent-{i}"} for i in range(65)],
    }
    atom = source_for(row)
    path = tmp_path / "restart.sqlite3"
    db = open_work(path, atom)
    restarted = set()
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        while fact_extraction_cursor(db, KEY)[1].category != "done":
            page = prepare_declared_source_page(db, KEY, source, action_budget=3)
            with write(db):
                apply_source_page(db, KEY, page)
            saved = fact_extraction_cursor(db, KEY)
            cursor = saved[1]
            if cursor.value_position and cursor.category not in restarted:
                restarted.add(cursor.category)
                db.close()
                atom = source_for(json.loads(canonical_json_bytes(row)))
                source = DeclaredAssertionSource(atom)
                db = open_work(path, atom)
                assert fact_extraction_cursor(db, KEY) == saved
        assert restarted == {"names", "profiles", "edges"}
        assert_projection(db, source)
    finally:
        db.close()


def test_one_name_is_projected_once_outside_transactions_while_fragments_span_pages(
    tmp_path, monkeypatch
):
    import riverhog_core.canonical_discovery_declared_source_work as owner

    row = {
        "assertion_id": "assertion",
        "type": "context",
        "evidence": [{"source_identifier": {"value": byte_name(b"long\\" * 10000)}}],
    }
    atom = source_for(row)
    db = open_work(tmp_path / "cached.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        calls = []
        actual = owner.name_representations

        def observed(value):
            assert not db.in_transaction
            calls.append(value)
            yield from actual(value)

        with monkeypatch.context() as patch:
            patch.setattr(owner, "name_representations", observed)
            drive_declared(db, source, 1)
        assert len(calls) == 1
        assert_projection(db, source)
    finally:
        db.close()


@pytest.mark.parametrize("category", ["profiles", "edges", "done"])
def test_each_category_terminal_rolls_back_the_entire_prepared_page_on_incomplete_facts(
    tmp_path, category
):
    row = {
        "assertion_id": "assertion",
        "type": "observation",
        "metadata": [{"name": byte_name(b"x")}],
        "profiles": [profile({})],
        "state": reference(),
    }
    atom = source_for(row)
    db = open_work(tmp_path / "terminals.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        before = fact_extraction_cursor(db, KEY)
        page = prepare_declared_source_page(db, KEY, source, action_budget=128)
        assert page.actions[-1].next_cursor.category == "done"
        totals = tuple(
            (kind, count + int(kind == category)) for kind, count in page.category_stream_totals
        )
        with write(db):
            with pytest.raises(StaleFactTask, match="admitted source terminal"):
                apply_source_page(db, KEY, replace(page, category_stream_totals=totals))
            assert fact_extraction_cursor(db, KEY) == before
            assert streams(db, "name-value") == {}
            assert fact_task_progress(db, KEY) == before[0]
        with write(db):
            apply_source_page(db, KEY, page)
        assert_projection(db, source)
    finally:
        db.close()


def test_interruption_after_name_and_profile_work_does_not_commit_a_partial_page(tmp_path):
    row = {
        "assertion_id": "assertion",
        "type": "context",
        "metadata": [{"name": byte_name(b"x")}],
        "profiles": [profile({})],
    }
    atom = source_for(row)
    db = open_work(tmp_path / "interrupt.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        before = fact_extraction_cursor(db, KEY)
        page = prepare_declared_source_page(db, KEY, source, action_budget=128)
        db.executescript("""
            CREATE TRIGGER stop_page BEFORE UPDATE ON discovery_fact_cursors
            WHEN NEW.category = 'edges'
            BEGIN SELECT RAISE(ABORT, 'interrupted category transition'); END;
        """)
        with write(db):
            with pytest.raises(sqlite3.IntegrityError, match="interrupted category transition"):
                apply_source_page(db, KEY, page)
            assert fact_extraction_cursor(db, KEY) == before
            assert streams(db, "profile-pin") == {}
        db.execute("DROP TRIGGER stop_page")
        with write(db):
            apply_source_page(db, KEY, page)
        assert_projection(db, source)
    finally:
        db.close()


def test_source_worker_and_version_fences_reject_changed_or_replayed_pages(tmp_path):
    row = {"assertion_id": "assertion", "type": "context"}
    atom = source_for(row)
    db = open_work(tmp_path / "fences.sqlite3", atom)
    try:
        drive(db, atom, 128)
        for changed in (
            source_for(row, "f" * 64),
            source_for({**row, "assertion_id": "other"}),
            source_for({**row, "profiles": []}),
            type(atom)("f" * 64, ORIGIN, row),
        ):
            with pytest.raises(StaleFactTask, match="obligation"):
                prepare_declared_source_page(
                    db, KEY, DeclaredAssertionSource(changed), action_budget=7
                )
        source = DeclaredAssertionSource(atom)
        page = prepare_declared_source_page(db, KEY, source, action_budget=7)
        with write(db), pytest.raises(StaleFactTask):
            apply_source_page(db, replace(KEY, worker_id="other"), page)
        with write(db):
            progress = apply_source_page(db, KEY, page)
        with write(db), pytest.raises(StaleFactTask, match="stale checkpoint"):
            apply_source_page(db, KEY, page)
        assert fact_task_progress(db, KEY) == progress
    finally:
        db.close()


@pytest.mark.parametrize("category", ["names", "profiles", "edges"])
def test_saved_grammar_position_cannot_skip_an_unvisited_source_record(tmp_path, category):
    row = {
        "assertion_id": "assertion",
        "type": "activity",
        "evidence": [{"source_identifier": {"value": byte_name(b"name")}}],
        "details": [profile({}), profile({})],
        "associations": [{"agent_id": "agent-0"}, {"agent_id": "agent-1"}],
    }
    atom = source_for(row)
    db = open_work(tmp_path / "skip.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        while fact_extraction_cursor(db, KEY)[1].category != category:
            page = prepare_declared_source_page(db, KEY, source, action_budget=1)
            with write(db):
                apply_source_page(db, KEY, page)
        _, cursor = fact_extraction_cursor(db, KEY)
        corrupted = replace(cursor, source_position=1)
        with pytest.raises(StaleFactTask, match="skipped|terminal"):
            while corrupted.category == category:
                corrupted = prepare_declared_source_step(source, corrupted).next_cursor
        if category == "names":
            with pytest.raises(StaleFactTask, match="subslot"):
                prepare_declared_source_step(source, replace(cursor, section_ordinal=6))
        with pytest.raises(StaleFactTask, match="continuation"):
            prepare_declared_source_step(source, replace(cursor, hash_state="unknown"))
        with pytest.raises(StaleFactTask, match="traversal"):
            prepare_declared_source_step(source, replace(cursor, stack_depth=1))
    finally:
        db.close()


@pytest.mark.parametrize("category", ["names", "profiles", "edges"])
def test_source_derived_stream_totals_detect_skips_even_when_record_counter_is_forged(
    tmp_path, category
):
    row = {
        "assertion_id": "assertion",
        "type": "activity",
        "evidence": [{"source_identifier": {"value": byte_name(b"name")}}],
        "details": [profile({})],
        "associations": [{"agent_id": "agent"}],
    }
    atom = source_for(row)
    db = open_work(tmp_path / "forged-terminal.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        while fact_extraction_cursor(db, KEY)[1].category != category:
            page = prepare_declared_source_page(db, KEY, source, action_budget=1)
            with write(db):
                apply_source_page(db, KEY, page)
        rules, terminal = {
            "names": (source.name_rules, atom.node_count + source.name_count),
            "profiles": (
                source.profile_rules,
                atom.node_count + source.name_count + source.profile_count,
            ),
            "edges": (
                source.edge_rules,
                atom.node_count + source.name_count + source.profile_count + source.edge_count,
            ),
        }[category]
        db.execute(
            "UPDATE discovery_fact_cursors SET source_node=?, source_position=0, "
            "section_ordinal=0, record_ordinal=?",
            (len(rules), terminal),
        )
        before = fact_extraction_cursor(db, KEY)
        page = prepare_declared_source_page(db, KEY, source, action_budget=128)
        with write(db):
            with pytest.raises(StaleFactTask, match="admitted source terminal"):
                apply_source_page(db, KEY, page)
            assert fact_extraction_cursor(db, KEY) == before
    finally:
        db.close()


@pytest.mark.parametrize("budget", [0, -1, True, 1.0])
def test_preparation_rejects_invalid_resource_budgets_without_mutation(tmp_path, budget):
    atom = source_for({"assertion_id": "assertion", "type": "context"})
    db = open_work(tmp_path / "invalid.sqlite3", atom)
    try:
        before = fact_extraction_cursor(db, KEY)
        with pytest.raises(ValueError, match="positive integer"):
            prepare_declared_source_page(
                db, KEY, DeclaredAssertionSource(atom), action_budget=budget
            )
        with write(db), pytest.raises(ValueError, match="outside"):
            prepare_declared_source_page(db, KEY, DeclaredAssertionSource(atom), action_budget=7)
        assert fact_extraction_cursor(db, KEY) == before
    finally:
        db.close()


def test_complete_native_source_keeps_opaque_lookalikes_lexical_and_finishes_all_categories(
    tmp_path,
):
    name = byte_name("source \U0001f99b".encode())
    data = {
        "metadata": [{"name": byte_name(b"fake")}],
        "profiles": [profile({})],
        "state": reference(),
    }
    fixture = native_profile_fixture(data, {"type": "object"}, source_names=(name,))
    atom = validated_source(fixture)
    db = open_work(tmp_path / "native.sqlite3", atom)
    try:
        drive(db, atom, 128)
        source = DeclaredAssertionSource(atom)
        assert source.name_count == 3
        assert source.profile_count == source.edge_count == 1
        drive_declared(db, source, 7)
        assert_projection(db, source)
        progress, _ = fact_extraction_cursor(db, KEY)
        with write(db):
            assert finish_assertion(
                db, KEY, source_terminal=atom.canonical_sha256, expected_version=progress.version
            ).complete
    finally:
        db.close()


@pytest.mark.growth_witness(
    "canonical-discovery-declared-source-page-preparation/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_declared_source_work.prepare_declared_source_page",
)
def test_measured_declared_pages_bound_sql_hashing_and_output_on_native_atoms(
    tmp_path, monkeypatch, record_property
):
    import riverhog_core.canonical_discovery_extraction as extraction
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    binding = growth_binding(prepare_declared_source_page)
    projection = project_bindings((binding,))
    declaration_sha = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_sha256 = hashlib.sha256
    samples = []
    for case, names, data in (
        ("empty-name", (byte_name(b""),), []),
        (
            "many-names",
            tuple(byte_name(f"source-{i}".encode()) for i in range(65)),
            {"opaque": byte_name(b"fake")},
        ),
        ("long-name", (byte_name("\U0001f99b".encode() * 5000),), {"opaque": [1, 2]}),
        ("opaque-invalid", (byte_name(b"\xff"), byte_name(b"opaque", "unknown")), {}),
    ):
        admitted = validated_source(native_profile_fixture(data, {}, source_names=names))
        for budget in (1, 7, 128):
            atom = source_for(admitted.row, admitted.origin_sha256)
            db = open_work(tmp_path / f"{case}-{budget}.sqlite3", atom)
            try:
                drive(db, atom, 128)
                source = DeclaredAssertionSource(atom)
                page_number = 0
                measured_categories = set()
                while fact_extraction_cursor(db, KEY)[1].category != "done":
                    _, before = fact_extraction_cursor(db, KEY)
                    selected = (
                        page_number in (0, 1, 7)
                        or before.category not in measured_categories
                        or before.value_position > 4096
                        and (before.category, "fragment") not in measured_categories
                    )
                    if selected:
                        measured_categories.add(before.category)
                        if before.value_position > 4096:
                            measured_categories.add((before.category, "fragment"))
                        recorder = RecordedConnection(db)

                        def observed_hash(value=b"", recorder=recorder, current_db=db, **kwargs):
                            assert not current_db.in_transaction
                            recorder.metrics["hashed_octets"] += len(value)
                            return actual_sha256(value, **kwargs)

                        with monkeypatch.context() as patch:
                            patch.setattr(extraction.hashlib, "sha256", observed_hash)
                            page = prepare_declared_source_page(
                                recorder, KEY, source, action_budget=budget
                            )
                        recorder.metrics["prepared_actions"] = len(page.actions)
                        recorder.metrics["prepared_fragment_octets"] = sum(
                            len(action.segment.exact_bytes)
                            for action in page.actions
                            if action.segment
                        )
                        sample = {
                            "declaration": binding.declaration.id,
                            "declaration_sha256": declaration_sha,
                            "dimensions": {
                                "action_budget": budget,
                                "atom_octets": len(atom.canonical_json),
                            },
                            "metrics": dict(recorder.metrics),
                            "delegates": {},
                        }
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
                    else:
                        page = prepare_declared_source_page(db, KEY, source, action_budget=budget)
                    with write(db):
                        apply_source_page(db, KEY, page)
                    page_number += 1
                assert_projection(db, source)
            finally:
                db.close()
    assert any(sample["metrics"]["hashed_octets"] > 0 for sample in samples)
    record_property("growth_samples", samples)
