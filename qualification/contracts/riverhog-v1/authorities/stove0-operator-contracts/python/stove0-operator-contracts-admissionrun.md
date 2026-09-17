# stove0_operator_contracts.AdmissionRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionrun:15d9669c48 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb4a406e6b"></a>
- <a id="s-2037711a36"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-55d2cf5a48"></a>`module`: `stove0_operator_contracts`
- <a id="s-a2f0c5be9f"></a>`name`: `AdmissionRun`
- <a id="s-67f208d163"></a>`unit`: `export`

### Declared structure

- <a id="s-0c6e627b36"></a>`kind`: `"class"`
- <a id="s-172451afe0"></a>`signature`: `"'(*, progressed: tuple[str, ...], failures: tuple[stove0_operator_contracts.SchedulerFailure, ...] = ()) -> None'"`

#### Validated model schema

<a id="s-4205c86be7"></a>

- <a id="s-121f3b37ec"></a>`type`: `"object"`
- <a id="s-68167a7fbd"></a>`additionalProperties`: `false`
- <a id="s-0812503572"></a>`required`: `["progressed"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-90d835b272"></a>`failures` | no | type="array"; default=[]; items=([SchedulerFailure](#s-109f10b819)) |  |
| <a id="s-0a310f71eb"></a>`progressed` | yes | type="array"; items=(type="string") |  |

##### Definitions

- [SchedulerFailure](#s-109f10b819)

##### <a id="s-109f10b819"></a>definition `SchedulerFailure`

- <a id="s-c41ae4d490"></a>`type`: `"object"`
- <a id="s-fab598d88c"></a>`additionalProperties`: `false`
- <a id="s-2384938150"></a>`required`: `["error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-af0171dba2"></a>`error` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-52be12e3fe"></a>`event_id` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-895387f455"></a>`work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

## Governing policies

- <a id="pa-7679e6dae4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionRun`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5abc96c6ec9ef5b27994b3d8d969a8db555952b6ac7305c3ba9b5821a3521b3e -->

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
  "name": "AdmissionRun",
  "unit": "export"
}
```

</details>
