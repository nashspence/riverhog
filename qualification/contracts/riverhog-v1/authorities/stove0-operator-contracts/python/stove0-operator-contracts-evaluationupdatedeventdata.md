# stove0_operator_contracts.EvaluationUpdatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdatedeventdata:1880c9f4b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8621a40e56"></a>
| Field | Shape |
|---|---|
| <a id="s-dc298e2657"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-64150470d7"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f11b35e119"></a>`module` | "stove0_operator_contracts" |
| <a id="s-ea15c17811"></a>`name` | "EvaluationUpdatedEventData" |
| <a id="s-5f20d84bf6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0a407365fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0295a12c05cf8f06d6083f44a6423fbcaa801e2f5973c4744610c7252fa4b7b8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "564ef285d77d3c9dad1bf60136e69ab8f2b535cfdc729c7830aa6ecdb46fd671",
    "signature": "\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationUpdatedEventData",
  "unit": "export"
}
```
