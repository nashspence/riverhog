# stove0_review_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-rcl-a341ddeeca:7f022b269c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3aa20faf3f"></a>
| Field | Shape |
|---|---|
| <a id="s-76733227c1"></a>`contract` | type="stove0_protocol.models.JsonSchemaDocument"; additional keys=`kind` |
| <a id="s-c4e0cf35e5"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-bfa1cd7774"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-e6275da69e"></a>`name` | "REVIEW_RCLONE_RECEIPT_SCHEMA" |
| <a id="s-3c8be64aeb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-df7abb42b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20f6f7638adef7559fd426839a364f3159c563b8b1a20482770527224dec4791 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaDocument"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_RCLONE_RECEIPT_SCHEMA",
  "unit": "export"
}
```
