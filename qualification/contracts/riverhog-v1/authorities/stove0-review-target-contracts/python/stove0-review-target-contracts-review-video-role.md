# stove0_review_target_contracts.REVIEW_VIDEO_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-video-role:eb2b731bc5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0375c58bc"></a>
| Field | Shape |
|---|---|
| <a id="s-c621d55dca"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-b11302337a"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-4f0eb58641"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-b93532a5c1"></a>`name` | "REVIEW_VIDEO_ROLE" |
| <a id="s-b45bdbb9d2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3a4646fd02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_VIDEO_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28a8aeb2c9cb885d5a2bd4ca0f19f68602eea067485431ab789282ca5860f356 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.video/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_VIDEO_ROLE",
  "unit": "export"
}
```
