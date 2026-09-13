# stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-mat-055d8dd392:3f326cdd02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82491f4ec6"></a>
| Field | Shape |
|---|---|
| <a id="s-bc9ff2b4a9"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-3994e79371"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-dbeedea2aa"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-41223c54ba"></a>`name` | "REVIEW_MATERIALIZE_INTENT_SCHEMA_ID" |
| <a id="s-614a78e0cf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4b55528ab2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22749fe28790d79683040283f5d194287b10fe7bcb5f08bdd73456d1a782301f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.materialize-intent/v1"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA_ID",
  "unit": "export"
}
```
