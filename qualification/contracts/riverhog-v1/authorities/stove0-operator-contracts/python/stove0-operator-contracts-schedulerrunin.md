# stove0_operator_contracts.SchedulerRunIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrunin:c97272be1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74446e0757"></a>
| Field | Shape |
|---|---|
| <a id="s-37ebe301bb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c36fa275bc"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-749f4a0346"></a>`module` | "stove0_operator_contracts" |
| <a id="s-0ad5cf17eb"></a>`name` | "SchedulerRunIn" |
| <a id="s-c9dbda8eb7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1b5efcf60b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRunIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06d8d8c011cdcd6df071686d017c3d1a1980bbbb94093b639990a6d17ad61902 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "bb6999171d296c228fdb5ff3ba85eb17a54fcbccdc9cfd7f0af37f0db3209dd5",
    "signature": "\"(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRunIn",
  "unit": "export"
}
```
