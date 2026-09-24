# stove0_operator_contracts.DepartureRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-departurerun:7314d0fc4e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5be639d329"></a>
- <a id="s-176186d581"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d92176673d"></a>`module`: `stove0_operator_contracts`
- <a id="s-f2f214db71"></a>`name`: `DepartureRun`
- <a id="s-5343737deb"></a>`unit`: `export`

### Declared structure

- <a id="s-2064ac1490"></a>`kind`: `"class"`
- <a id="s-7e6f5b197a"></a>`signature`: `"'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-de778c261b"></a>

- <a id="s-2dd9f84583"></a>`type`: `"object"`
- <a id="s-aa482df4e8"></a>`additionalProperties`: `false`
- <a id="s-70e78f74e6"></a>`required`: `["progressed"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8480a7dda5"></a>`failures` | no | type="array"; default=[]; items=([SchedulerFailure](#s-5fbe9b459f)) |  |
| <a id="s-613298a430"></a>`progressed` | yes | type="array"; items=(type="string") |  |

##### Definitions

- [SchedulerFailure](#s-5fbe9b459f)

##### <a id="s-5fbe9b459f"></a>definition `SchedulerFailure`

- <a id="s-706d73dfa9"></a>`type`: `"object"`
- <a id="s-f7377285b2"></a>`additionalProperties`: `false`
- <a id="s-8ed18fc368"></a>`required`: `["error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-117539a58a"></a>`error` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-7a526e9dd2"></a>`event_id` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-413a2b3f17"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

## Governing policies

- <a id="pa-19b4412155"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.DepartureRun`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e762e0709a228b30e7de86eebb2b96f675cec87f67f08072dc29213407940a4 -->

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
        "failures": {
          "default": [],
          "items": {
            "$ref": "#/$defs/SchedulerFailure"
          },
          "type": "array"
        },
        "progressed": {
          "items": {
            "type": "string"
          },
          "type": "array"
        }
      },
      "required": [
        "progressed"
      ],
      "type": "object"
    },
    "signature": "'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "DepartureRun",
  "unit": "export"
}
```

</details>
