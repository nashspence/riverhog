# stove0_media_sampling_observer_contracts.MediaSamplingFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-265fa88d55:a8595ad000 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85e1d1eea3"></a>
| Field | Shape |
|---|---|
| <a id="s-38d45e06bc"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-dc30fb5161"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-cfacc32298"></a>`module` | "stove0_media_sampling_observer_contracts" |
| <a id="s-eadc555b52"></a>`name` | "MediaSamplingFacts" |
| <a id="s-0474688481"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_sampling_observer_contracts.MediaSamplingFacts.canonical_artifacts](stove0-media-sampling-observer-contracts-mediasamplingfacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-bd7dbcbe06"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.MediaSamplingFacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f0d347a6c98a77ce5fbb0714816c3bd5e0c41936cba97fea63631fff12102b5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "45f5c3bef4df9ef057e1a8a3dcee1ed2ba40debbda2c5eeba7cf216039c6d7f5",
    "signature": "'(*, artifacts: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "MediaSamplingFacts",
  "unit": "export"
}
```
