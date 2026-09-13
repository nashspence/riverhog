# stove0_operator_contracts.EvaluationChildView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationchildview:6b3914916b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-12b69a71fc"></a>
| Field | Shape |
|---|---|
| <a id="s-d25348b9f9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c6ac2a566b"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-59c72a8c72"></a>`module` | "stove0_operator_contracts" |
| <a id="s-1d7f238327"></a>`name` | "EvaluationChildView" |
| <a id="s-6f1530b59b"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationChildView.exact_output](stove0-operator-contracts-evaluationchildview-exact-output.md)

## Governing policies

- <a id="pa-7bf47f15e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationChildView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efa85b949efdf3c4306f6cd40be3ba3168f6624912b14e50d89544a05c863ebe -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d842ebe048284b0a96af0ab5da247cccef935c71e3371ed98e3fddbd3ce7e762",
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['pending', 'active', 'complete', 'inapplicable', 'failed', 'canceled'], output: stove0_target_protocol.protocol.OutputCollectionRef | None = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationChildView",
  "unit": "export"
}
```
