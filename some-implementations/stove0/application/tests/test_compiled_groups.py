"""Whole-scope grouping keeps exact members and checkpoints its large domains."""

from __future__ import annotations

import hashlib

import pytest
from stove0_core.compiled_observations import CompiledObservationPlanning
from stove0_core.observation_questions import physical_question
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_protocol.interfaces import seal_owned_interface
from stove0_protocol import ArtifactSelection, WorkIdentity, WorkPayload, canonical_json_sha256
from stove0_protocol.input_groups import update_input_group_commitment
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    ObserverImplementation,
)
from stove0_protocol.observation_interfaces import ObservationInterfaceVector
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import ObserverResource, RecipeDependencyCatalog
from stove0_recipe_config.source import RecipeSource
from test_accepted_observations import _subject


def _program(*, fork=None):
    row = {
        "type": "object",
        "properties": {
            "subject_id": {"type": "string"},
            "kind": {"type": "string"},
        },
        "required": ["subject_id", "kind"],
        "additionalProperties": False,
    }
    edge = {
        "type": "object",
        "properties": {
            "primary": {"type": "string"},
            "associated": {"type": "string"},
        },
        "required": ["primary", "associated"],
        "additionalProperties": False,
    }
    status = {
        "type": "object",
        "properties": {
            "subject": {"type": "string"},
            "state": {"type": "string"},
        },
        "required": ["subject", "state"],
        "additionalProperties": False,
    }
    properties = {
        "artifacts": {"type": "array", "items": row},
        "preferred_edges": {"type": "array", "items": edge},
        "fallback_edges": {"type": "array", "items": edge},
        "preferred_status": {"type": "array", "items": status},
        "fallback_status": {"type": "array", "items": status},
    }
    contract = ObserverContract.seal(
        ObserverContractPayload(
            id="example.relations/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "example.options/v1",
                {
                    "type": "object",
                    "additionalProperties": False,
                },
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "example.relation-facts/v1",
                {
                    "type": "object",
                    "properties": properties,
                    "required": sorted(properties),
                    "additionalProperties": False,
                },
            ),
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )
    views = {
        "artifacts": {
            "kind": "subject-facts",
            "records": {"records_at": "/artifacts"},
            "subject_at": "/subject_id",
            "record_schema_at": "/properties/artifacts/items",
        }
    }
    for name in ("preferred", "fallback"):
        views[name] = {
            "kind": "relation",
            "records": {"records_at": f"/{name}_edges"},
            "where": True,
            "require": True,
            "primary": {"kind": "subject-id", "at": "/primary"},
            "associated": {"kind": "subject-id", "at": "/associated"},
            "coverage": ["subjects"],
            "status": {
                "kind": "records",
                "records_at": f"/{name}_status",
                "subject_at": "/subject",
                "value_at": "/state",
                "values": {
                    value: value
                    for value in ("complete", "unsupported", "ambiguous", "insufficient")
                },
            },
        }
    sample = _facts((_subject(0), _subject(1)), ("primary", "associated"))
    interface, vectors = seal_owned_interface(
        contract=contract,
        id="example.relation-interface/v1",
        inputs={
            "subjects": {
                "kind": "subjects",
            }
        },
        views=views,
        partitioning="whole-scope",
        empty_scope="complete-empty",
        vectors=(
            ObservationInterfaceVector(
                id="accepted",
                accepted=True,
                options={},
                subjects=(_subject(0), _subject(1)),
                facts=sample,
            ),
            ObservationInterfaceVector(
                id="missing",
                accepted=False,
                options={},
                subjects=(_subject(0), _subject(1)),
                facts={**sample, "artifacts": []},
            ),
        ),
    )
    resource = ObserverResource(contract=contract, interface=interface, interface_vectors=vectors)
    cases = [
        {
            "role": role,
            "when": {
                "facts": {
                    "view": "relations.artifacts",
                    "scope": "self",
                    "quantifier": "any",
                    "where": {"test": {"path": "/kind", "op": "eq", "value": role}},
                }
            },
        }
        for role in ("primary", "associated")
    ]
    from test_compiled_program import _catalog

    recipe, closure = compile_recipe(
        RecipeSource.model_validate(
            {
                "format": "stove0-recipe/v1",
                "id": "example.groups/v1",
                "revision": 1,
                "roles": {"primary": "example.primary/v1", "associated": "example.associated/v1"},
                "observe": {"relations": {"use": "relations"}},
                "classify": {"cases": cases, "otherwise": None},
                "groups": {
                    "media": {
                        "primary": "primary",
                        "attach": ["associated"],
                        "prefer": ["relations.preferred", "relations.fallback"],
                    },
                    "primaries": {"primary": "primary"},
                },
                "decisions": [
                    {
                        "when": False,
                        "no_output": {
                            "code": "example.unused/v1",
                            "message": "No matching decision.",
                        },
                    }
                ],
                "fork": fork or {},
            }
        ),
        RecipeDependencyCatalog(
            resources={"relations": resource, "effect": _catalog().resources["effect"]}
        ),
    )
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.relation-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    return recipe, closure, resource, descriptor


def _facts(subjects, kinds, *, preferred=(), fallback=(), statuses=None):
    facts = {
        "artifacts": [
            {"subject_id": member.id, "kind": kind}
            for member, kind in zip(subjects, kinds, strict=True)
        ]
    }
    for name, edges in (("preferred", preferred), ("fallback", fallback)):
        facts[f"{name}_edges"] = [
            {"primary": subjects[primary].id, "associated": subjects[associated].id}
            for primary, associated in edges
        ]
        facts[f"{name}_status"] = [
            {"subject": member.id, "state": (statuses or {}).get((name, index), "complete")}
            for index, member in enumerate(subjects)
        ]
    return facts


def _observed_state(tmp_path, kinds, *, fork=None, **fact_options):
    recipe, closure, resource, descriptor = _program(fork=fork)
    subjects = tuple(_subject(index) for index in range(len(kinds)))
    inventory = ArtifactSelection.seal(subjects)
    work = WorkIdentity.seal(
        WorkPayload(recipe=recipe.ref, inputs=inventory.roots(), effective_intent={})
    )
    url = f"sqlite:///{tmp_path / 'state.db'}"
    state = SqlAlchemyStateStore(url)
    state.recipe_definitions.retain(recipe, closure)
    state.retain_selection(inventory)
    row = state.compiled_planning.ensure(work.work_id, recipe)
    state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=inventory.ref()
    )
    answered = False
    for _ in range(100):
        progress = CompiledObservationPlanning(state, object()).step(work)
        if progress.state == "question" and not answered:
            question = progress.question
            request = physical_question(
                question=question,
                interface=resource.interface,
                registration_id="relations",
                descriptor=descriptor,
                subjects=subjects,
                subject_ports={"subjects": tuple(member.id for member in subjects)},
                evidence_ports={},
            )
            facts = _facts(subjects, kinds, **fact_options)
            result = ContentObservationResult.seal(
                ContentObservationResultPayload(
                    request_id=request.request_id,
                    state="observed",
                    observer=ObserverImplementation(
                        id=descriptor.implementation_id,
                        version=descriptor.implementation_version,
                        source_revision=descriptor.source_revision,
                        descriptor_sha256=descriptor.descriptor_sha256,
                    ),
                    observer_contract_id=resource.contract.id,
                    observer_contract_sha256=resource.contract.contract_sha256,
                    subjects=request.subjects,
                    facts_schema=resource.contract.facts_schema,
                    facts=facts,
                    facts_sha256=canonical_json_sha256(facts),
                )
            )
            state.accepted_observations.register_request(
                question, request, descriptor, resource.interface
            )
            state.accepted_observations.accept(
                ContentObservationEvidence(request=request, result=result),
                contract=resource.contract,
                interface=resource.interface,
                subject_ports={"subjects": tuple(member.id for member in subjects)},
            )
            answered = True
        if progress.state == "complete":
            break
    else:
        pytest.fail("relation observation graph did not complete")
    return state, url, work, subjects


def _groups(state, url, work, group_id="media"):
    for steps in range(3000):
        authority = state.compiled_groups.step(work, group_id)
        if authority is not None:
            return state, authority, steps
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    pytest.fail("group metadata stopped making bounded progress")


def test_unknown_primary_status_blocks_every_potentially_affected_primary(tmp_path):
    state, url, work, _ = _observed_state(
        tmp_path,
        ("primary", "primary", "associated"),
        preferred=((0, 2),),
        statuses={("preferred", 1): "unsupported"},
    )
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == 0
    state.engine.dispose()


def test_no_associated_members_keeps_all_exact_primaries(tmp_path):
    state, url, work, subjects = _observed_state(tmp_path, ("primary", "primary"))
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == 2 and authority.association_count == 0
    page = state.compiled_groups.page(authority, start_ordinal=0)
    assert [member.primary_id for member in page.members] == [subject.id for subject in subjects]
    state.engine.dispose()


def test_large_group_paging_candidate_and_restart_keep_equal_byte_instances_distinct(tmp_path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary",) + ("associated",) * 205 + ("ignored",),
        preferred=tuple((0, index) for index in range(1, 206)),
    )
    state, authority, steps = _groups(state, url, work)
    assert authority.group_count == 1 and authority.association_count == 205
    assert steps > 205  # Real continuations, including multiple membership pages.
    digest, records, ordinal = hashlib.sha256(), [], 0
    while True:
        page = state.compiled_groups.page(authority, start_ordinal=ordinal)
        assert len(page.members) <= 100
        for record in page.members:
            update_input_group_commitment(digest, ordinal=ordinal, member=record)
            records.append(record)
            ordinal += 1
        if page.complete:
            break
    assert digest.hexdigest() == authority.members_sha256
    assert records[0].primary_id == subjects[0].id and records[0].associated_id is None
    assert {record.associated_id for record in records[1:]} == {
        member.id for member in subjects[1:206]
    }
    for _ in range(30):
        selection = state.compiled_groups.candidate(authority, subjects[0].id)
        if selection is not None:
            break
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    else:
        pytest.fail("candidate selection stopped making bounded progress")
    assert selection.artifact_count == 206
    selected = tuple(state.iter_selection_artifacts(selection.selection_sha256))
    assert len({member.id for member in selected}) == 206
    assert len({member.sha256 for member in selected}) == 1
    assert all(
        member.role in {"example.primary/v1", "example.associated/v1"} for member in selected
    )
    with pytest.raises(ValueError, match="unblocked sealed primary"):
        state.compiled_groups.candidate(authority, subjects[-1].id)
    with pytest.raises(ValueError, match="exact member extent"):
        state.compiled_groups.page(authority, start_ordinal=207)
    state.engine.dispose()


@pytest.mark.parametrize("state_value", ["unsupported", "ambiguous", "insufficient"])
def test_nonnegative_stronger_relation_never_falls_through_to_weaker_match(tmp_path, state_value):
    state, url, work, _ = _observed_state(
        tmp_path,
        ("primary", "primary", "associated"),
        fallback=((0, 2),),
        statuses={("preferred", 2): state_value},
    )
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == 0 and authority.association_count == 0
    assert state.compiled_groups.page(authority, start_ordinal=0).complete
    state.engine.dispose()


def test_complete_negative_uses_weaker_tier_and_empty_primary_group_survives(tmp_path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "primary", "associated", "associated"),
        preferred=((1, 3),),
        fallback=((0, 2), (0, 3)),
    )
    state, authority, _ = _groups(state, url, work)
    page = state.compiled_groups.page(authority, start_ordinal=0)
    assert [(member.primary_id, member.associated_id) for member in page.members] == [
        (subjects[0].id, None),
        (subjects[0].id, subjects[2].id),
        (subjects[1].id, None),
        (subjects[1].id, subjects[3].id),
    ]
    state, primaries, _ = _groups(state, url, work, "primaries")
    assert primaries.group_count == 2 and primaries.association_count == 0
    assert all(
        member.associated_id is None
        for member in state.compiled_groups.page(primaries, start_ordinal=0).members
    )
    state.engine.dispose()


def test_positive_stronger_relation_to_excluded_primary_prevents_weaker_reinterpretation(tmp_path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "ignored", "associated"),
        preferred=((1, 2),),
        fallback=((0, 2),),
    )
    state, authority, _ = _groups(state, url, work)
    page = state.compiled_groups.page(authority, start_ordinal=0)
    assert [(member.primary_id, member.associated_id) for member in page.members] == [
        (subjects[0].id, None)
    ]
    assert authority.association_count == 0
    # Exclusion leaves original members available to exact coverage accounting.
    inventory = state.compiled_planning.inventory_ref(work.work_id)
    assert inventory.artifact_count == 3
    state.engine.dispose()


def test_ambiguous_primary_matches_are_blocked_in_pages_and_others_remain_visible(tmp_path):
    kinds = ("primary",) * 121 + ("associated",)
    state, url, work, subjects = _observed_state(
        tmp_path,
        kinds,
        preferred=tuple((index, 121) for index in range(120)),
        fallback=((120, 121),),
    )
    state, authority, _ = _groups(state, url, work)
    assert authority.group_count == 1 and authority.association_count == 0
    page = state.compiled_groups.page(authority, start_ordinal=0)
    assert page.complete and page.members[0].primary_id == subjects[120].id
    with pytest.raises(ValueError, match="unblocked sealed primary"):
        state.compiled_groups.candidate(authority, subjects[0].id)
    state.engine.dispose()
