# stove0_operator_contracts.EvaluationView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationview:cccfaf14d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fb913ef36"></a>
| Field | Shape |
|---|---|
| <a id="s-ffb79f5328"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-691e724c39"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-b51ab2d827"></a>`module` | "stove0_operator_contracts" |
| <a id="s-3f1b79eda1"></a>`name` | "EvaluationView" |
| <a id="s-1381c3117d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationView.exact_identity](stove0-operator-contracts-evaluationview-exact-identity.md)
- [stove0_operator_contracts.EvaluationView.from_record](stove0-operator-contracts-evaluationview-from-record.md)

## Governing policies

- <a id="pa-9f8bca2a83"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b50387ff41a2407d2f020a31f96ece6d1ef9b4fe34c7c5a085943cb81e7debf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "cac11576d6501d999cc52f3a6eb7b9749f72f524c0bb8797e80bc27396840637",
    "signature": "\"(*, format: Literal['stove0-evaluation-view/v1'] = 'stove0-evaluation-view/v1', evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], definition: stove0_protocol.models.EvaluationDefinition, phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=1)], children: tuple[stove0_operator_contracts.EvaluationChildView, ...], reviews: tuple[stove0_operator_contracts.EvaluationReviewView, ...] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationView",
  "unit": "export"
}
```
