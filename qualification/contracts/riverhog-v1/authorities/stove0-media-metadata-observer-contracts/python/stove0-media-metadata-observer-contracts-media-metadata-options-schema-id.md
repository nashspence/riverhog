# stove0_media_metadata_observer_contracts.MEDIA_METADATA_OPTIONS_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-b03be5512e:314eaf6ca1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0089f3b573"></a>
| Field | Shape |
|---|---|
| <a id="s-0e2c56144e"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-dd2bea444b"></a>`distribution` | "stove0-media-metadata-observer-contracts" |
| <a id="s-ebc53a0033"></a>`module` | "stove0_media_metadata_observer_contracts" |
| <a id="s-3c5a5e3ed7"></a>`name` | "MEDIA_METADATA_OPTIONS_SCHEMA_ID" |
| <a id="s-0134a86fc9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9053edf2b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MEDIA_METADATA_OPTIONS_SCHEMA_ID`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b6ed3733dfb3a704259e55f73672fb5665429d141567cde7d89ed216177cc75 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.metadata-options/v1"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MEDIA_METADATA_OPTIONS_SCHEMA_ID",
  "unit": "export"
}
```
