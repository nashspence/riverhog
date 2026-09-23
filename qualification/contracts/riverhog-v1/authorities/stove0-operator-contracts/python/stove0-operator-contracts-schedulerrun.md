# stove0_operator_contracts.SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerrun:7c5d6153ae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9052785ed5"></a>
- <a id="s-e6d0614c12"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-3af0525ee1"></a>`module`: `stove0_operator_contracts`
- <a id="s-a21009d9bc"></a>`name`: `SchedulerRun`
- <a id="s-7d4de8abc1"></a>`unit`: `export`

### Declared structure

- <a id="s-707b5c1086"></a>`kind`: `"class"`
- <a id="s-ac3c4af9b1"></a>`signature`: `"'(*, pruning: stove0_operator_contracts.SchedulerPruning \| None, admission: stove0_operator_contracts.AdmissionRun \| None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None'"`

#### Validated model schema

<a id="s-bea95f16ce"></a>

- <a id="s-c7db1dc77d"></a>`type`: `"object"`
- <a id="s-ccf4a0a0bd"></a>`additionalProperties`: `false`
- <a id="s-ed13546b9e"></a>`required`: `["pruning","work"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4529d53325"></a>`admission` | no | anyOf=[([AdmissionRun](#s-bba32c184c)); (type="null")]; default=null |  |
| <a id="s-d0ff3529cf"></a>`pruning` | yes | anyOf=[([SchedulerPruning](#s-bc7006be46)); (type="null")] |  |
| <a id="s-557354c3af"></a>`work` | yes | [SchedulerWorkBatch](#s-85f50ba4ce) |  |

##### Definitions

- [AdmissionRun](#s-bba32c184c)
- [SchedulerFailure](#s-f931be92db)
- [SchedulerPruning](#s-bc7006be46)
- [SchedulerWorkBatch](#s-85f50ba4ce)

##### <a id="s-bba32c184c"></a>definition `AdmissionRun`

- <a id="s-43fd4a0103"></a>`type`: `"object"`
- <a id="s-752b8dc46d"></a>`additionalProperties`: `false`
- <a id="s-9353c60274"></a>`required`: `["progressed"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dcac46254d"></a>`failures` | no | type="array"; default=[]; items=([SchedulerFailure](#s-f931be92db)) |  |
| <a id="s-5720136508"></a>`progressed` | yes | type="array"; items=(type="string") |  |

##### <a id="s-f931be92db"></a>definition `SchedulerFailure`

- <a id="s-a2b7b3701d"></a>`type`: `"object"`
- <a id="s-f351027049"></a>`additionalProperties`: `false`
- <a id="s-23b1103e41"></a>`required`: `["error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-512d9ab40f"></a>`error` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-13280c6b71"></a>`event_id` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-75e71774bf"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### <a id="s-bc7006be46"></a>definition `SchedulerPruning`

- <a id="s-557c53c486"></a>`type`: `"object"`
- <a id="s-dfed785bee"></a>`additionalProperties`: `false`
- <a id="s-861def0134"></a>`required`: `["work","work_bytes","evaluations","evaluation_bytes","selections","selection_bytes","events","event_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c49ada348c"></a>`evaluation_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1d33d965d7"></a>`evaluations` | yes | type="integer"; minimum=0 |  |
| <a id="s-169eacceb3"></a>`event_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-283e0cec4a"></a>`events` | yes | type="integer"; minimum=0 |  |
| <a id="s-ed7b295716"></a>`selection_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-99367b5ff3"></a>`selections` | yes | type="integer"; minimum=0 |  |
| <a id="s-96c5fc98bd"></a>`work` | yes | type="integer"; minimum=0 |  |
| <a id="s-a763f69d36"></a>`work_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-85f50ba4ce"></a>definition `SchedulerWorkBatch`

- <a id="s-d46866def3"></a>`type`: `"object"`
- <a id="s-6b95ba490b"></a>`additionalProperties`: `false`
- <a id="s-461dbd2a8f"></a>`required`: `["role","cursor","next_cursor","progressed","failures"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-febaa7cdbe"></a>`cursor` | yes | type="string" |  |
| <a id="s-83204bb86d"></a>`failures` | yes | type="array"; items=([SchedulerFailure](#s-f931be92db)) |  |
| <a id="s-71bf1bf982"></a>`next_cursor` | yes | type="string" |  |
| <a id="s-491358acb9"></a>`progressed` | yes | type="array"; items=(type="string"; pattern="^[0-9a-f]{64}$") |  |
| <a id="s-d2bfd7f1c2"></a>`role` | yes | type="string"; enum=["controller","worker","combined"] |  |

## Governing policies

- <a id="pa-324752f5b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerRun`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7871590768273f2167839930d35587f3bce0f42c659838b422c6e9c1cd7e06e0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdmissionRun": {
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
        },
        "SchedulerPruning": {
          "additionalProperties": false,
          "properties": {
            "evaluation_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "evaluations": {
              "minimum": 0,
              "type": "integer"
            },
            "event_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "events": {
              "minimum": 0,
              "type": "integer"
            },
            "selection_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "selections": {
              "minimum": 0,
              "type": "integer"
            },
            "work": {
              "minimum": 0,
              "type": "integer"
            },
            "work_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "work",
            "work_bytes",
            "evaluations",
            "evaluation_bytes",
            "selections",
            "selection_bytes",
            "events",
            "event_bytes"
          ],
          "type": "object"
        },
        "SchedulerWorkBatch": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "admission": {
          "anyOf": [
            {
              "$ref": "#/$defs/AdmissionRun"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "pruning": {
          "anyOf": [
            {
              "$ref": "#/$defs/SchedulerPruning"
            },
            {
              "type": "null"
            }
          ]
        },
        "work": {
          "$ref": "#/$defs/SchedulerWorkBatch"
        }
      },
      "required": [
        "pruning",
        "work"
      ],
      "type": "object"
    },
    "signature": "'(*, pruning: stove0_operator_contracts.SchedulerPruning | None, admission: stove0_operator_contracts.AdmissionRun | None = None, work: stove0_operator_contracts.SchedulerWorkBatch) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerRun",
  "unit": "export"
}
```

</details>
