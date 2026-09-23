# stove0_operator_contracts.SchedulerRunIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrunin:c97272be1b -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-29eab23810"></a>`type`: `"object"`
- <a id="s-44f1b93d31"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2bbbfb2c7f"></a>`role` | no | type="string"; enum=["controller","worker","combined"]; default="combined" |  |
| <a id="s-361bcb3516"></a>`work_limit` | no | type="integer"; minimum=1; maximum=100; default=25 |  |

## Governing policies

- <a id="pa-1b5efcf60b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRunIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cdece75f4b1297ea50e602aec77c1579a6d2e2dce16b1bebda4716bed8c3d2b -->

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
          "type": "string"
        },
        "work_limit": {
          "default": 25,
          "maximum": 100,
          "minimum": 1,
          "type": "integer"
        }
      },
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

</details>
