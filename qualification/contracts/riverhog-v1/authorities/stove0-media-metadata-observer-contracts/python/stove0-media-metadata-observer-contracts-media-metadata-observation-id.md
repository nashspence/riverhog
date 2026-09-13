# stove0_media_metadata_observer_contracts.MEDIA_METADATA_OBSERVATION_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-6cdc7e5908:a681a9bcb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8aa1da4394"></a>
| Field | Shape |
|---|---|
| <a id="s-c7f6a91ea7"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-501df85ebb"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-9bc8115353"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-9d85164695"></a>`name` | "MEDIA_METADATA_OBSERVATION_ID" |
| <a id="s-292a94a54a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8a5ff372c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MEDIA_METADATA_OBSERVATION_ID`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a912a61f9b0df7ad6b59d5788c56150a157c8a58117169d2040092b5a262ae8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.metadata/v1"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MEDIA_METADATA_OBSERVATION_ID",
  "unit": "export"
}
```
