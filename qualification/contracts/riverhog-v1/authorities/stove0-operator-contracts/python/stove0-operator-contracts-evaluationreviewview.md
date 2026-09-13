# stove0_operator_contracts.EvaluationReviewView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationreviewview:457deed3dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b2a530699"></a>
| Field | Shape |
|---|---|
| <a id="s-a0d22e54ba"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-25f42785a7"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f8c8c722c8"></a>`module` | "stove0_operator_contracts" |
| <a id="s-36e8466b46"></a>`name` | "EvaluationReviewView" |
| <a id="s-30dde84072"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationReviewView.meaningful](stove0-operator-contracts-evaluationreviewview-meaningful.md)

## Governing policies

- <a id="pa-d3d4bba199"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationReviewView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19164fb58cf84d0ba3ca0063a07ef9b42715d41fd91a7ee75ffbb3c57ee84f1c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5f6ace515468312142e6d3c1a30f7cec5850a1b90a5a169d4582ad88301f5e14",
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationReviewView",
  "unit": "export"
}
```
