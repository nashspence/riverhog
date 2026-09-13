# stove0_media_archive_target_contracts.validate_audio_archive_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-val-a14c92ebad:5e558074eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5188cf02de"></a>
| Field | Shape |
|---|---|
| <a id="s-a4c52334f4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d3f95439f2"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-6851d351c6"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-80051231cc"></a>`name` | "validate_audio_archive_intent" |
| <a id="s-5c99ea25a4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b73fa4a306"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.validate_audio_archive_intent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad0a4e6871fba9f6c98f3651f6baef6872a87cdfef48e2e314a78116c95bd981 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "validate_audio_archive_intent",
  "unit": "export"
}
```
