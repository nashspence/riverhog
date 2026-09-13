# stove0_review_target_contracts.validate_review_materialize_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-validate-r-aaa0977d3c:abc10c6d45 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c87196a92"></a>
| Field | Shape |
|---|---|
| <a id="s-75c62df2d8"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ea7d71f605"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-0e855bdfb2"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-3a02a7273e"></a>`name` | "validate_review_materialize_intent" |
| <a id="s-04517725fd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4cd9126cdc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.validate_review_materialize_intent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7291aebd2fa280e0b128a80f6f4c94f8d3a3603b97c51f4a05d76a26e8b7c202 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "validate_review_materialize_intent",
  "unit": "export"
}
```
