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
- <a id="s-c36fa275bc"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-749f4a0346"></a>`module`: `stove0_operator_contracts`
- <a id="s-0ad5cf17eb"></a>`name`: `SchedulerRunIn`
- <a id="s-c9dbda8eb7"></a>`unit`: `export`

### Declared structure

- <a id="s-ea48552522"></a>`kind`: `"class"`
- <a id="s-f32885fcc4"></a>`signature`: `"\"(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None\""`

#### Validated model schema

<a id="s-650df8a463"></a>
- <a id="s-b7a0b3d779"></a>`title`: SchedulerRunIn
- <a id="s-29eab23810"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2bbbfb2c7f"></a>`role` | no | type="string"; enum=["controller","worker","combined"] |  |
| <a id="s-361bcb3516"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100 |  |

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

<!-- exact-contract-value: 673b7f010eb6c00232c5429dfb26f3351656dee5268915bb29d08c4cb06e9812 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "role": {
          "default": "combined",
          "enum": [
            "controller",
            "worker",
            "combined"
          ],
          "title": "Role",
          "type": "string"
        },
        "work_limit": {
          "default": 25,
          "maximum": 100,
          "minimum": 1,
          "title": "Work Limit",
          "type": "integer"
        }
      },
      "title": "SchedulerRunIn",
      "type": "object"
    },
    "signature": "\"(*, role: Literal['controller', 'worker', 'combined'] = 'combined', work_limit: Annotated[int, Ge(ge=1), Le(le=100)] = 25) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRunIn",
  "unit": "export"
}
```
