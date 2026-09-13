# stove0_media_metadata_observer_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts:e37d90105f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-280c8691dc"></a>
| Field | Shape |
|---|---|
| <a id="s-8d3ae7b658"></a>`candidate_id` | "python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts" |
| <a id="s-9672e3cecb"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-72fb9d73c8"></a>`exports` | additional keys=`MEDIA_METADATA_FACTS_CONFORMANCE_VECTORS`, `MEDIA_METADATA_FACTS_SCHEMA`, `MEDIA_METADATA_FACTS_SCHEMA_ID`, `MEDIA_METADATA_FACTS_SEMANTICS`, `MEDIA_METADATA_OBSERVATION_ID`, `MEDIA_METADATA_OBSERVER_CONTRACT`, `MEDIA_METADATA_OPTIONS_SCHEMA`, `MEDIA_METADATA_OPTIONS_SCHEMA_ID`, `MEDIA_METADATA_SEMANTIC_VALIDATOR`, `MediaArtifactFacts`, `MediaArtifactState`, `MediaFactEvidence`, `MediaFactName`, `MediaMetadataFact`, `MediaMetadataFacts`, `validate_media_metadata_facts`, `validate_media_metadata_observation` |
| <a id="s-d6e0c303df"></a>`module` | "stove0_media_metadata_observer_contracts" |

## Governing policies

- <a id="pa-cf91bdf6fa"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb47524da42c5de8f648782ed9071d2d8335319a536906998c9595cf3d0be6fd -->

```json
{
  "candidate_id": "python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts",
  "distribution": "stove0-media-metadata-observer-contracts",
  "exports": {
    "MEDIA_METADATA_FACTS_CONFORMANCE_VECTORS": {
      "kind": "object",
      "type": "stove0_observer_protocol.conformance.SemanticFactsConformanceVectors"
    },
    "MEDIA_METADATA_FACTS_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "MEDIA_METADATA_FACTS_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.media.metadata-facts/v1"
    },
    "MEDIA_METADATA_FACTS_SEMANTICS": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "MEDIA_METADATA_OBSERVATION_ID": {
      "kind": "constant",
      "value": "stove0.media.metadata/v1"
    },
    "MEDIA_METADATA_OBSERVER_CONTRACT": {
      "kind": "object",
      "type": "stove0_protocol.models.ObserverContract"
    },
    "MEDIA_METADATA_OPTIONS_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "MEDIA_METADATA_OPTIONS_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.media.metadata-options/v1"
    },
    "MEDIA_METADATA_SEMANTIC_VALIDATOR": {
      "kind": "object",
      "type": "stove0_observer_protocol.validation.SemanticValidatorBinding"
    },
    "MediaArtifactFacts": {
      "kind": "class",
      "members": {
        "valid_state": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "64a2c3c20961d6282bdf02f4727399391df9329ca3212b0457d35d6f5dd60ce2",
      "signature": "\"(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], state: Literal['observed', 'unsupported'], facts: tuple[stove0_media_metadata_observer_contracts.contracts.MediaMetadataFact, ...] = ()) -> None\""
    },
    "MediaArtifactState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "MediaFactEvidence": {
      "kind": "class",
      "schema_sha256": "aaa797b126a9df34f6f9669a9b4a92fdad30c0811310a1ccc3086865f377cc37",
      "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], field: Annotated[str, MinLen(min_length=1), MaxLen(max_length=240)]) -> None'"
    },
    "MediaFactName": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "MediaMetadataFact": {
      "kind": "class",
      "schema_sha256": "6bf6c513c1eaffa8790cfceb1ed51efcb1005ba23283931318c474e2d251aca0",
      "signature": "\"(*, name: Literal['capture-time', 'container-format', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], value: JsonValue, evidence: stove0_media_metadata_observer_contracts.contracts.MediaFactEvidence) -> None\""
    },
    "MediaMetadataFacts": {
      "kind": "class",
      "members": {
        "canonical_artifacts": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[MediaArtifactFacts, ...]') -> 'tuple[MediaArtifactFacts, ...]'\""
        }
      },
      "schema_sha256": "624c9687b02b9d304bb6fb8f726e6de14b7a9dae1132e4f689254cdba020d73e",
      "signature": "'(*, artifacts: Annotated[tuple[stove0_media_metadata_observer_contracts.contracts.MediaArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
    },
    "validate_media_metadata_facts": {
      "kind": "function",
      "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaMetadataFacts'\""
    },
    "validate_media_metadata_observation": {
      "kind": "function",
      "signature": "\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
    }
  },
  "module": "stove0_media_metadata_observer_contracts"
}
```
