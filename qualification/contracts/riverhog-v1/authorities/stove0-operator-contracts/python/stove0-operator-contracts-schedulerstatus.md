# stove0_operator_contracts.SchedulerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerstatus:0cc34ce399 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d85b8fcc4"></a>
- <a id="s-744391c4fd"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-ab919852d7"></a>`module`: `stove0_operator_contracts`
- <a id="s-da05acebf9"></a>`name`: `SchedulerStatus`
- <a id="s-506bb2f405"></a>`unit`: `export`

### Declared structure

- <a id="s-c563c0d6d5"></a>`kind`: `"class"`
- <a id="s-1f5029262b"></a>`signature`: `"\"(*, running: bool, interval_seconds: Annotated[float, Gt(gt=0)], roles: tuple[typing.Literal['controller', 'worker', 'combined'], ...]) -> None\""`

#### Validated model schema

<a id="s-cf090b61a4"></a>

- <a id="s-0684c55048"></a>`type`: `"object"`
- <a id="s-1a39f5db34"></a>`additionalProperties`: `false`
- <a id="s-e49661ad98"></a>`required`: `["running","interval_seconds","roles"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6fb25e8804"></a>`interval_seconds` | yes | type="number"; exclusiveMinimum=0 |  |
| <a id="s-c938f628bc"></a>`roles` | yes | type="array"; items=(type="string"; enum=["controller","worker","combined"]) |  |
| <a id="s-f4889d9b36"></a>`running` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-23dd0659f0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4fcad434f3fdb1df17b6fd1a817409065f130f3563b8b609c7b4d4e1745b01e7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "interval_seconds": {
          "exclusiveMinimum": 0,
          "type": "number"
        },
        "roles": {
          "items": {
            "enum": [
              "controller",
              "worker",
              "combined"
            ],
            "type": "string"
          },
          "type": "array"
        },
        "running": {
          "type": "boolean"
        }
      },
      "required": [
        "running",
        "interval_seconds",
        "roles"
      ],
      "type": "object"
    },
    "signature": "\"(*, running: bool, interval_seconds: Annotated[float, Gt(gt=0)], roles: tuple[typing.Literal['controller', 'worker', 'combined'], ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerStatus",
  "unit": "export"
}
```

</details>
