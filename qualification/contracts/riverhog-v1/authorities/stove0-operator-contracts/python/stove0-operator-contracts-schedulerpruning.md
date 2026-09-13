# stove0_operator_contracts.SchedulerPruning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerpruning:e65e25c26a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c550b1ab90"></a>
| Field | Shape |
|---|---|
| <a id="s-25ff813397"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b864aac556"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-f8517daba3"></a>`module` | "stove0_operator_contracts" |
| <a id="s-c3fa11ca62"></a>`name` | "SchedulerPruning" |
| <a id="s-a8376fc91d"></a>`unit` | "export" |

## Governing policies

- <a id="pa-56a4c0121a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerPruning`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1a051e912d57937767b1d327f10bdd14fbdb8d99ec7261c8175c1dcb2128937 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "639614e3a2443f84fe63c2477757b5c11d526671cd5d37014cf1699993cccc8c",
    "signature": "'(*, work: Annotated[int, Ge(ge=0)], work_bytes: Annotated[int, Ge(ge=0)], evaluations: Annotated[int, Ge(ge=0)], evaluation_bytes: Annotated[int, Ge(ge=0)], selections: Annotated[int, Ge(ge=0)], selection_bytes: Annotated[int, Ge(ge=0)], events: Annotated[int, Ge(ge=0)], event_bytes: Annotated[int, Ge(ge=0)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerPruning",
  "unit": "export"
}
```
