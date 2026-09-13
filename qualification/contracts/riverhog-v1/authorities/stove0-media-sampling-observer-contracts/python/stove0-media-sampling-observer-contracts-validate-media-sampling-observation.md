# stove0_media_sampling_observer_contracts.validate_media_sampling_observation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-79b6ffd88d:c4695be24b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7032d8e868"></a>
| Field | Shape |
|---|---|
| <a id="s-1ea1aca2c4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ac86f6d51f"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-7d207a412b"></a>`module` | "stove0_media_sampling_observer_contracts" |
| <a id="s-bc0efbafc1"></a>`name` | "validate_media_sampling_observation" |
| <a id="s-04d1de84ec"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c6714ef04d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.validate_media_sampling_observation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33a6169c43b3c5f182a1c76394713727cc7decad36d5759dffb84b6be87a911c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObservationRequest', facts: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "validate_media_sampling_observation",
  "unit": "export"
}
```
