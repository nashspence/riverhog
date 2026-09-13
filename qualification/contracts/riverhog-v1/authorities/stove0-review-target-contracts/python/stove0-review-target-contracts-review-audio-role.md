# stove0_review_target_contracts.REVIEW_AUDIO_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-audio-role:879412a9a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23573a8d6d"></a>
| Field | Shape |
|---|---|
| <a id="s-697c63c8e1"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-16a1379c7a"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-bf202c3f4a"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-fccf24349e"></a>`name` | "REVIEW_AUDIO_ROLE" |
| <a id="s-1285b4e3ae"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fa6648c45a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_AUDIO_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a465725b4bb34aa52f8404a2d3ceecea7cae03c0e47c3eaf413b0c1c35fafac -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.audio/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_AUDIO_ROLE",
  "unit": "export"
}
```
