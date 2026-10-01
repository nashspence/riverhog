"""Physical plans for native request-body selectors over a large projection."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from riverhog_core.canonical_discovery_index import EXTRACTION_CONTRACT_SHA256
from riverhog_core.canonical_discovery_search import (
    _discovery_candidate_statement,
    _selected_collections,
)
from riverhog_protocol import ArtifactDiscoveryRequest
from riverhog_provenance_contracts import core_contract
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from tests.support.qualification.database_selector_plans import PlanCase


def seed_native_discovery_relations(engine: Engine) -> None:
    """Assume one validated index generation; separately qualify actual staging."""
    with engine.begin() as connection:
        connection.execute(
            text("""
            INSERT INTO collection_provenance_index_generations (
                build_id, collection_id, archive_generation, archive_root_sha256,
                provenance_identity, core_contract_sha256, extraction_contract_sha256,
                dataset_sha256, generation_id, complete, expected_epoch, created_at
            )
            SELECT lpad(id::text, 36, '0'), id, archive_generation, archive_root_sha256,
                   provenance_identity, :core, :extraction, archive_root_sha256,
                   archive_root_sha256, true, 1, created_at
            FROM collections
        """),
            {"core": core_contract().contract_sha256, "extraction": EXTRACTION_CONTRACT_SHA256},
        )
        for statement in (
            """
            INSERT INTO collection_provenance_index_state (
                collection_id, epoch, active_build_id, phase
            ) SELECT id, 1, lpad(id::text, 36, '0'), 'ready' FROM collections
            """,
            """
            INSERT INTO collection_provenance_index_snapshots (
                build_id, journal_id, prefix_sha256, prefix_bytes, through_entry_id,
                through_sequence, through_json_sha256, assertion_count
            ) SELECT build_id, 'urn:uuid:11111111-1111-4111-8111-111111111111',
                     repeat('a', 64), 1, 'urn:uuid:22222222-2222-4222-8222-222222222222',
                     0, repeat('b', 64), 1 FROM collection_provenance_index_generations
            """,
            """
            INSERT INTO collection_provenance_index_entries (
                build_id, journal_id, prefix_sha256, sequence, entry_id, json_sha256, entry_kind
            ) SELECT build_id, journal_id, prefix_sha256, 0, through_entry_id,
                     through_json_sha256, 'assertions' FROM collection_provenance_index_snapshots
            """,
            """
            INSERT INTO collection_provenance_index_members (
                build_id, artifact_id, bytes, sha256, journal_id,
                prefix_sha256, delivery_association_id
            ) SELECT lpad(collection_id::text, 36, '0'), artifact_id, bytes, sha256,
                     'urn:uuid:11111111-1111-4111-8111-111111111111', repeat('a', 64),
                     'urn:uuid:33333333-3333-4333-8333-333333333333'
              FROM collection_artifacts
            """,
            """
            INSERT INTO collection_provenance_index_assertions (
                build_id, row_key, journal_id, prefix_sha256, sequence, entry_id,
                assertion_id, referent_id, kind, assertion_state, assertion_sha256, canonical_json
            ) SELECT build_id, artifact_id, journal_id, prefix_sha256, 0,
                     'urn:uuid:22222222-2222-4222-8222-222222222222',
                     'urn:uuid:33333333-3333-4333-8333-333333333333',
                     'urn:uuid:44444444-4444-4444-8444-444444444444',
                     'reported_description', 'effective', artifact_id, convert_to('{}', 'UTF8')
              FROM collection_provenance_index_members
            """,
            """
            INSERT INTO collection_provenance_index_memberships (
                build_id, artifact_id, row_key, scope
            ) SELECT build_id, artifact_id, artifact_id, 'member'
              FROM collection_provenance_index_members
            """,
            """
            INSERT INTO collection_provenance_index_profiles (
                build_id, row_key, pointer, contract_id, contract_sha256, schema_id
            ) SELECT build_id, row_key, '/properties', 'qualification:opaque', repeat('c', 64),
                     'qualification:section' FROM collection_provenance_index_assertions
            """,
            """
            INSERT INTO collection_provenance_index_values (
                build_id, row_key, ordinal, pointer, representation, scalar_type,
                source_value_sha256, exact_json, text_value, folded_text
            ) SELECT build_id, row_key, 0, '/properties/data/name', 'json-scalar', 'string',
                     row_key, convert_to('"' || row_key || '"', 'UTF8'), row_key, row_key
              FROM collection_provenance_index_assertions
            """,
            """
            INSERT INTO collection_provenance_index_text_chunks (
                build_id, row_key, value_ordinal, chunk_ordinal, codepoint_offset,
                text_chunk, folded_chunk
            ) SELECT build_id, row_key, 0, 0, 0, text_value, folded_text
              FROM collection_provenance_index_values
            """,
            "ANALYZE",
        ):
            connection.execute(text(statement))


def native_discovery_plan_cases(engine: Engine) -> Iterator[PlanCase]:
    member = hashlib.md5(b"extra-42", usedforsecurity=False).hexdigest() * 2
    digest = hashlib.md5(b"42", usedforsecurity=False).hexdigest() * 2
    requests = {
        "collections": {"collections": ["1"]},
        "tags_all": {"tags_all": ["tag-000001"]},
        "tags_any": {"tags_any": ["tag-000001"]},
        "tags_none": {"tags_none": ["tag-000001"]},
        "description_contains": {"description_contains": "description 000001"},
        "artifact_id": {"artifact_id": member},
        "payload_sha256": {"payload_sha256": digest},
    }
    for name, changes in requests.items():
        request = ArtifactDiscoveryRequest.model_validate(changes)
        with Session(engine) as session:
            statement = (
                _discovery_candidate_statement(
                    _selected_collections(session, request, None), request=request, principal=None
                )
                .order_by("collection_id", "artifact_id")
                .limit(100)
            )
        yield PlanCase("discovery.body." + name, statement, expected_nodes=frozenset({"Limit"}))
    for name, changes in {
        "contains": {},
        "equals": {"operator": "equals"},
        "ascii_fold": {"text_mode": "ascii-fold"},
        "pointer": {"pointer": "/properties/data/name"},
        "profile": {},
        "retracted": {},
    }.items():
        clause = {
            "scopes": ["member"],
            "kind": "reported_description",
            "values": [{"value": member, **changes}],
        }
        if name == "profile":
            clause["profile"] = {
                "contract_id": "qualification:opaque",
                "contract_sha256": "c" * 64,
                "schema_id": "qualification:section",
            }
        if name == "retracted":
            clause["assertion_state"] = "retracted"
        request = ArtifactDiscoveryRequest.model_validate(
            {"collections": ["1"], "provenance_all": [clause]}
        )
        with Session(engine) as session:
            statement = (
                _discovery_candidate_statement(
                    _selected_collections(session, request, None), request=request, principal=None
                )
                .order_by("collection_id", "artifact_id")
                .limit(100)
            )
        yield PlanCase(
            "discovery.body.provenance_all." + name,
            statement,
            expected_indexes=frozenset(
                {
                    "collection_provenance_index_memberships_pkey",
                    "ix_provenance_index_membership_row",
                }
            ),
        )
