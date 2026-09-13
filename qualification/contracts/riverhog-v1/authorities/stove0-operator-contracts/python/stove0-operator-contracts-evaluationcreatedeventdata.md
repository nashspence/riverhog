# stove0_operator_contracts.EvaluationCreatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreatedeventdata:04246e15b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-674496c0ef"></a>
| Field | Shape |
|---|---|
| <a id="s-159796f237"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-be23190b94"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-59e18ca097"></a>`module` | "stove0_operator_contracts" |
| <a id="s-773daf55cd"></a>`name` | "EvaluationCreatedEventData" |
| <a id="s-b4174158a4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6edce65f56"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 206fe437ff132b26c5408273e3f5d6a7814b4d9d532222ccb34c448bba6b9c61 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8a95a196acf8a76a51c6b3ef5fe4f5abffb52508d5830fac6aac0ff2138ae10a",
    "signature": "\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled']) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationCreatedEventData",
  "unit": "export"
}
```
