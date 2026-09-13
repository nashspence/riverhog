# stove0_media_archive_target_contracts.AV1_OPUS_ARCHIVE_INTENT_SEMANTICS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-av1-b86e8e55f5:03a7192f14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a98d6277e"></a>
| Field | Shape |
|---|---|
| <a id="s-2fe5c754e6"></a>`contract` | type="stove0_protocol.models.SemanticValidationProfile"; additional keys=`kind` |
| <a id="s-3ad0ab0b43"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-ebe049fd95"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-f7447b09cd"></a>`name` | "AV1_OPUS_ARCHIVE_INTENT_SEMANTICS" |
| <a id="s-cda55c154f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e9a2c0d81b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.AV1_OPUS_ARCHIVE_INTENT_SEMANTICS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b40babae66657b1a3f7b61e9f59db11bbee4c3d8249f853d5187700e47ab41e6 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.SemanticValidationProfile"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "AV1_OPUS_ARCHIVE_INTENT_SEMANTICS",
  "unit": "export"
}
```
