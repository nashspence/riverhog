# stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_optional_text

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-ef4aed578c:0635841b17 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b840f54a0f"></a>
| Field | Shape |
|---|---|
| <a id="s-16c7f8f387"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d02980a29b"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-f7d93969d6"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-db952848eb"></a>`name` | "canonical_optional_text" |
| <a id="s-7d09a1e9b4"></a>`owner` | "stove0_media_archive_target_contracts.MediaProjectionPolicy" |
| <a id="s-71ad933d24"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaProjectionPolicy](stove0-media-archive-target-contracts-mediaprojectionpolicy.md)

## Governing policies

- <a id="pa-f9061e6ef1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaProjectionPolicy.canonical_optional_text`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f716ae7533ac83035c4c5c5f12baccaa52595cb5e73f83824182c906daafb183 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str | None') -> 'str | None'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "canonical_optional_text",
  "owner": "stove0_media_archive_target_contracts.MediaProjectionPolicy",
  "unit": "member"
}
```
