from __future__ import annotations

import copy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter, ValidationError
from riverhog_provenance_contracts import (
    DIALECT,
    ENTRY_SCHEMA,
    PROFILE,
    ContractCatalog,
    EntryReference,
    ExternalReference,
    LocalReference,
    ProvenanceContractBinding,
    ProvenanceId,
    canonical_document,
    core_contract,
    core_schemas,
    decode_document,
    profile_reference,
    require_portable_json,
    schema_document,
)

ID = "urn:uuid:12345678-1234-1234-1234-123456789abc"


def test_all_schemas_are_draft_2020_12_and_closed_reference_pack():
    pack = core_contract()
    assert len(pack.schemas) >= 12
    for schema in pack.schemas.values():
        assert schema["$schema"] == DIALECT
        Draft202012Validator.check_schema(schema)
    assert pack.schema_dialect == DIALECT
    assert pack.format_policy == "annotation-only"


def test_schema_documents_are_isolated_copies():
    first = core_schemas()
    first[ENTRY_SCHEMA]["title"] = "tampered"
    assert core_schemas()[ENTRY_SCHEMA]["title"] != "tampered"
    ref = profile_reference(PROFILE + "/profiles/filesystem-observation.schema.json")
    assert ref["contract_sha256"] == core_contract().contract_sha256


@pytest.mark.parametrize(
    "value",
    [
        None,
        1.0,
        float("nan"),
        float("inf"),
        2**53,
        {"x": None},
        {"x": "bad\x00"},
        "\ud800",
        "\ufffe",
        "\ufdd0",
        {1: "key"},
    ],
)
def test_nonportable_wire_values_are_rejected(value):
    with pytest.raises((ValueError, TypeError)):
        require_portable_json(value)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"x":1,"x":2}',
        b'{"x":9007199254740993}',
        b'{"x":NaN}',
        b'{"x":"\\ud800"}',
        b'{"x":"\\u0000"}',
        b'{"x":null}',
        b'{"x":1.5}',
        b"[]",
        b'{"x": 1}',
        b"\xef\xbb\xbf{}",
    ],
)
def test_raw_json_rejects_loss_or_noncanonical_frames(raw):
    with pytest.raises(ValueError):
        decode_document(raw)


def test_canonical_object_keys_are_utf16_ordered_and_unicode_is_not_normalized():
    raw = canonical_document({"\U0001f600": "x", "\ue000": "y", "e\u0301": "accent"})
    assert raw.index("\U0001f600".encode()) < raw.index("\ue000".encode())
    assert b"e\xcc\x81" in raw
    assert decode_document(raw)["e\u0301"] == "accent"


@pytest.mark.parametrize(
    "value", ["URN:UUID:12345678-1234-1234-1234-123456789abc", ID + "\n", ID.upper(), ID[9:], "x"]
)
def test_identity_aliases_are_rejected(value):
    with pytest.raises(ValidationError):
        TypeAdapter(ProvenanceId).validate_python(value)


def test_exact_reference_types_have_no_current_state_or_path():
    entry = EntryReference(entry_id=ID, sequence="0", json_sha256="a" * 64)
    local = LocalReference(object_id=ID, object_type="state")
    foreign = ExternalReference(
        journal_id=ID, entry=entry, assertion_id=ID, object_id=ID, object_type="state"
    )
    assert local.model_dump()["scope"] == "local"
    assert foreign.model_dump()["scope"] == "external"
    for extra in ({"current_state_id": ID}, {"path": "/x"}):
        with pytest.raises(ValidationError):
            LocalReference(object_id=ID, object_type="state", **extra)


@pytest.mark.parametrize("sequence", ["-1", "00", "+1", "1\n", str(2**63)])
def test_sequence_reference_has_exact_bounded_decimal_grammar(sequence):
    with pytest.raises(ValidationError):
        EntryReference(entry_id=ID, sequence=sequence, json_sha256="a" * 64)


def test_sealed_contract_digest_is_deterministic_and_not_mutable():
    schema = {"$schema": DIALECT, "$id": "https://example.test/a", "type": "string"}
    a = ProvenanceContractBinding(contract_id="test", schemas=[schema])
    b = ProvenanceContractBinding(contract_id="test", schemas=[copy.deepcopy(schema)])
    assert a.contract_sha256 == b.contract_sha256
    schema["type"] = "integer"
    assert a.schemas["https://example.test/a"]["type"] == "string"
    mutable = a.schemas
    mutable["https://example.test/a"]["type"] = "boolean"
    assert a.schemas["https://example.test/a"]["type"] == "string"


@pytest.mark.parametrize(
    "mutation", ["duplicate", "missing-id", "bad-dialect", "missing-ref", "remote-ref", "nested-id"]
)
def test_unsealed_schema_packs_are_rejected(mutation):
    schema = {"$schema": DIALECT, "$id": "https://example.test/a", "type": "object"}
    schemas = [schema]
    if mutation == "duplicate":
        schemas.append(copy.deepcopy(schema))
    elif mutation == "missing-id":
        del schema["$id"]
    elif mutation == "bad-dialect":
        schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    elif mutation == "missing-ref":
        schema["$ref"] = "#/$defs/nope"
    elif mutation == "remote-ref":
        schema["$ref"] = "https://example.test/not-in-pack"
    else:
        schema["properties"] = {"a": {"$id": "https://example.test/nested", "type": "string"}}
    with pytest.raises(ValueError):
        ProvenanceContractBinding(contract_id="test", schemas=schemas)


def test_cross_schema_references_resolve_without_network():
    first = {"$schema": DIALECT, "$id": "https://example.test/a", "$ref": "https://example.test/b"}
    second = {"$schema": DIALECT, "$id": "https://example.test/b", "type": "string", "minLength": 1}
    pack = ProvenanceContractBinding(contract_id="test", schemas=[first, second])
    catalog = ContractCatalog([pack])
    catalog.validate(first["$id"], "valid")
    with pytest.raises(ValueError):
        catalog.validate(first["$id"], "")


def test_profile_hash_is_not_interchangeable_with_schema_name():
    schema_id = PROFILE + "/profiles/filesystem-observation.schema.json"
    ref = profile_reference(schema_id)
    ref["contract_sha256"] = "0" * 64
    assert ContractCatalog().validate_profile(ref, {}) is False


def test_packages_and_wire_version_are_not_incremented():
    import tomllib

    packages = Path(__file__).resolve().parents[2]
    for name in ("riverhog-provenance", "riverhog-provenance-contracts"):
        data = tomllib.loads((packages / name / "pyproject.toml").read_text())
        assert data["project"]["version"] == "0.1.0"
    assert schema_document()["$defs"]["journalEntry"]["properties"]["schema_version"] == {
        "const": "1.0.0"
    }
