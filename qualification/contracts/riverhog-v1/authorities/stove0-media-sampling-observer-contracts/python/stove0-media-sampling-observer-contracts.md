# stove0_media_sampling_observer_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts:ff326750dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-020e7ee701) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-706fa6d926"></a>
| Field | Shape |
|---|---|
| <a id="s-353d54d1eb"></a>`candidate_id` | "python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts" |
| <a id="s-2365477ae4"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-26f96dd67d"></a>`exports` | additional keys=`MEDIA_SAMPLING_FACTS_CONFORMANCE_VECTORS`, `MEDIA_SAMPLING_FACTS_SCHEMA`, `MEDIA_SAMPLING_FACTS_SCHEMA_ID`, `MEDIA_SAMPLING_FACTS_SEMANTICS`, `MEDIA_SAMPLING_OBSERVATION_ID`, `MEDIA_SAMPLING_OBSERVER_CONTRACT`, `MEDIA_SAMPLING_OPTIONS_SCHEMA`, `MEDIA_SAMPLING_OPTIONS_SCHEMA_ID`, `MEDIA_SAMPLING_SEMANTIC_VALIDATOR`, `MediaSamplingArtifactFacts`, `MediaSamplingFacts`, `SampleableRange`, `validate_media_sampling_facts`, `validate_media_sampling_observation` |
| <a id="s-5ae8c11c8b"></a>`module` | "stove0_media_sampling_observer_contracts" |

## Governing policies

- <a id="pa-f0595565cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3632646b0fb933a108bf66e7b2e217f3e0c35af8c40b43f1da261adf65c96fe -->

```json
{
  "candidate_id": "python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts",
  "distribution": "stove0-media-sampling-observer-contracts",
  "exports": {
    "MEDIA_SAMPLING_FACTS_CONFORMANCE_VECTORS": {
      "kind": "object",
      "type": "stove0_observer_protocol.conformance.SemanticFactsConformanceVectors"
    },
    "MEDIA_SAMPLING_FACTS_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "MEDIA_SAMPLING_FACTS_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.review.media-sampling-facts/v1"
    },
    "MEDIA_SAMPLING_FACTS_SEMANTICS": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "MEDIA_SAMPLING_OBSERVATION_ID": {
      "kind": "constant",
      "value": "stove0.review.media-sampling/v1"
    },
    "MEDIA_SAMPLING_OBSERVER_CONTRACT": {
      "kind": "object",
      "type": "stove0_protocol.models.ObserverContract"
    },
    "MEDIA_SAMPLING_OPTIONS_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "MEDIA_SAMPLING_OPTIONS_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.review.media-sampling-options/v1"
    },
    "MEDIA_SAMPLING_SEMANTIC_VALIDATOR": {
      "kind": "object",
      "type": "stove0_observer_protocol.validation.SemanticValidatorBinding"
    },
    "MediaSamplingArtifactFacts": {
      "kind": "class",
      "members": {
        "validate_ranges": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "94887d96e49df2efae09e9520e4bda3993b49202807d4d242ac22c277735f2c8",
      "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"
    },
    "MediaSamplingFacts": {
      "kind": "class",
      "members": {
        "canonical_artifacts": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[MediaSamplingArtifactFacts, ...]') -> 'tuple[MediaSamplingArtifactFacts, ...]'\""
        }
      },
      "schema_sha256": "45f5c3bef4df9ef057e1a8a3dcee1ed2ba40debbda2c5eeba7cf216039c6d7f5",
      "signature": "'(*, artifacts: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
    },
    "SampleableRange": {
      "kind": "class",
      "schema_sha256": "0f6c17a4144ba9bfacd01e668546fd8d744823cf27a929ea5e9a11839f6be09d",
      "signature": "'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
    },
    "validate_media_sampling_facts": {
      "kind": "function",
      "signature": "\"(facts: 'Mapping[str, object]', subjects: 'Sequence[ArtifactSubject]') -> 'MediaSamplingFacts'\""
    },
    "validate_media_sampling_observation": {
      "kind": "function",
      "signature": "\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
    }
  },
  "module": "stove0_media_sampling_observer_contracts"
}
```
