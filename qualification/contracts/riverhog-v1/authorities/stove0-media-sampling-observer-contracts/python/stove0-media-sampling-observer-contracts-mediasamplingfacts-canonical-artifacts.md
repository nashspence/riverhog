# stove0_media_sampling_observer_contracts.MediaSamplingFacts.canonical_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-222c45873b:31a9b27b69 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3db3615017"></a>
| Field | Shape |
|---|---|
| <a id="s-f91803485f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3ce0e15d8c"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-f0a7ec9384"></a>`module` | "stove0_media_sampling_observer_contracts" |
| <a id="s-8d42fd1bd6"></a>`name` | "canonical_artifacts" |
| <a id="s-6a61893db5"></a>`owner` | "stove0_media_sampling_observer_contracts.MediaSamplingFacts" |
| <a id="s-d7aa09911c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_media_sampling_observer_contracts.MediaSamplingFacts](stove0-media-sampling-observer-contracts-mediasamplingfacts.md)

## Governing policies

- <a id="pa-2444b39e9f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.MediaSamplingFacts.canonical_artifacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cbb0e3939d3d4e12554a083174cdb64c3cdc960aac4a0e5fbeebf1470b717d9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[MediaSamplingArtifactFacts, ...]') -> 'tuple[MediaSamplingArtifactFacts, ...]'\""
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "canonical_artifacts",
  "owner": "stove0_media_sampling_observer_contracts.MediaSamplingFacts",
  "unit": "member"
}
```
