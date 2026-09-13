# stove0_review_target_support.create_target_app

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-create-target-app:e60ac300d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff3a20d8c7"></a>
| Field | Shape |
|---|---|
| <a id="s-fff20404d3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-55d248c43d"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-614929fd1c"></a>`module` | "stove0_review_target_support" |
| <a id="s-d1c5fd686e"></a>`name` | "create_target_app" |
| <a id="s-d7c70f1a1c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d477808f7e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.create_target_app`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fac72b90c831e92e91f034e35b338b92069688b40d3e1e093de09789ef550043 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, service: 'str', title: 'str', token: 'str', target: 'ReviewTarget') -> 'FastAPI'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "create_target_app",
  "unit": "export"
}
```
