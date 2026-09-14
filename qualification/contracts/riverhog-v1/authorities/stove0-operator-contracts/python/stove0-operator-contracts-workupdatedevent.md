# stove0_operator_contracts.WorkUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedevent:b70963456b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f34ed81392"></a>
- <a id="s-3d8a289b38"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-3e154e63b5"></a>`module`: `stove0_operator_contracts`
- <a id="s-80b374214b"></a>`name`: `WorkUpdatedEvent`
- <a id="s-ea297efab6"></a>`unit`: `export`

### Declared structure

- <a id="s-8161f95009"></a>`kind`: `"class"`
- <a id="s-f078f699e6"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkUpdatedEventData) -> None\""`

#### Validated model schema

<a id="s-d0016e645b"></a>
- <a id="s-2580168585"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-99f463106a"></a>`data` | yes | #/$defs/WorkUpdatedEventData |  |
| <a id="s-5e209602b9"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-76a67b34ed"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-b22c31709e"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-7172069c73"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-6ace36c6ad"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-1e8e6facd4"></a>`time` | yes | type="string" |  |
| <a id="s-8d8ae1ff1e"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.updated" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-2e033a7744"></a>`WorkUpdatedEventData` | type="object"; fields=`phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-ab31ef4dcf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 22577fefce7bce3bb2f88de6855409a4e8dc4e412da74f47cee30011fc78b596 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WorkUpdatedEventData": {
          "additionalProperties": false,
          "properties": {
            "phase": {
              "enum": [
                "eligible",
                "claimed",
                "observing",
                "planning",
                "target_preflight",
                "queued",
                "executing",
                "output_finalizing",
                "verifying",
                "settled",
                "retirement_pending",
                "coordinating",
                "abandon_pending",
                "complete",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase",
            "revision"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/WorkUpdatedEventData"
        },
        "datacontenttype": {
          "const": "application/json",
          "default": "application/json",
          "type": "string"
        },
        "id": {
          "minLength": 1,
          "type": "string"
        },
        "source": {
          "const": "urn:riverhog:stove0",
          "type": "string"
        },
        "specversion": {
          "const": "1.0",
          "default": "1.0",
          "type": "string"
        },
        "subject": {
          "minLength": 1,
          "type": "string"
        },
        "time": {
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.work.updated",
          "type": "string"
        }
      },
      "required": [
        "id",
        "source",
        "type",
        "subject",
        "time",
        "data"
      ],
      "type": "object"
    },
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkUpdatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkUpdatedEvent",
  "unit": "export"
}
```
