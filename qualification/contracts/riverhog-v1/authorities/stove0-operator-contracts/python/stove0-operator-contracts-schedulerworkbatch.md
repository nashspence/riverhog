# stove0_operator_contracts.SchedulerWorkBatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerworkbatch:afc2db9aed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d876608274"></a>
- <a id="s-9870bed9cd"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9628750893"></a>`module`: `stove0_operator_contracts`
- <a id="s-f7c8a8072b"></a>`name`: `SchedulerWorkBatch`
- <a id="s-3b3255e2fc"></a>`unit`: `export`

### Declared structure

- <a id="s-893b3b8741"></a>`kind`: `"class"`
- <a id="s-fda32eeaa9"></a>`signature`: `"\"(*, role: Literal['controller', 'worker', 'combined'], cursor: str, next_cursor: str, progressed: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...]) -> None\""`

#### Validated model schema

<a id="s-32572a0437"></a>

- <a id="s-4bc9da636a"></a>`type`: `"object"`
- <a id="s-6bc01b557b"></a>`additionalProperties`: `false`
- <a id="s-31a027b38a"></a>`required`: `["role","cursor","next_cursor","progressed","failures"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1f4c295e8"></a>`cursor` | yes | type="string" |  |
| <a id="s-355109a36f"></a>`failures` | yes | type="array"; items=([SchedulerFailure](#s-2747364e28)) |  |
| <a id="s-b0d7b8b4a6"></a>`next_cursor` | yes | type="string" |  |
| <a id="s-ef32b8f15b"></a>`progressed` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-95adbfea4b"></a>`role` | yes | type="string"; enum=["controller","worker","combined"] |  |

##### Definitions

- [SchedulerFailure](#s-2747364e28)

##### <a id="s-2747364e28"></a>definition `SchedulerFailure`

- <a id="s-d99f93a642"></a>`type`: `"object"`
- <a id="s-c70c5faaa3"></a>`additionalProperties`: `false`
- <a id="s-610011052b"></a>`required`: `["error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-104475c1eb"></a>`error` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-303f2323bf"></a>`event_id` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-1a4f5f47cf"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

## Governing policies

- <a id="pa-fe676e9736"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerWorkBatch`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c73f9fe3afc50cdfa5f2dc6f1eed2e9e61d022624622c55ed166d87b7cf19550 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "SchedulerFailure": {
          "additionalProperties": false,
          "properties": {
            "error": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "event_id": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "work_id": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            }
          },
          "required": [
            "error"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "cursor": {
          "type": "string"
        },
        "failures": {
          "items": {
            "$ref": "#/$defs/SchedulerFailure"
          },
          "type": "array"
        },
        "next_cursor": {
          "type": "string"
        },
        "progressed": {
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "type": "array"
        },
        "role": {
          "enum": [
            "controller",
            "worker",
            "combined"
          ],
          "type": "string"
        }
      },
      "required": [
        "role",
        "cursor",
        "next_cursor",
        "progressed",
        "failures"
      ],
      "type": "object"
    },
    "signature": "\"(*, role: Literal['controller', 'worker', 'combined'], cursor: str, next_cursor: str, progressed: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerWorkBatch",
  "unit": "export"
}
```

</details>
