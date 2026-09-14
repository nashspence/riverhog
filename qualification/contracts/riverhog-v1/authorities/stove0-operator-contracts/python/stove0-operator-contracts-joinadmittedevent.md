# stove0_operator_contracts.JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedevent:c2d272407b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2fd208df44"></a>
- <a id="s-17313ffa83"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-bf41737332"></a>`module`: `stove0_operator_contracts`
- <a id="s-42e8593db4"></a>`name`: `JoinAdmittedEvent`
- <a id="s-e33552aba3"></a>`unit`: `export`

### Declared structure

- <a id="s-97be2a0ce6"></a>`kind`: `"class"`
- <a id="s-59a3993686"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None\""`

#### Validated model schema

<a id="s-bac1e80528"></a>
- <a id="s-c5ed972d66"></a>`title`: JoinAdmittedEvent
- <a id="s-931ec71f9e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6869980525"></a>`data` | yes | #/$defs/JoinAdmittedEventData |  |
| <a id="s-785e024407"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-ccb2be9233"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-51011c4491"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-d2654e8452"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-3b54a9a1b2"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-56769ad228"></a>`time` | yes | type="string" |  |
| <a id="s-73450a5e5b"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-1698a3ec14"></a>`JoinAdmittedEventData` | type="object"; fields=`branch_set_sha256`, `join_plan_sha256`, `join_work_id`, `phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-729dab7ac8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ceeb7bbfd14cde8bf37ccefbc63953df6b66ef121c730dc73ca49f3b683d4dbf -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JoinAdmittedEventData": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Branch Set Sha256",
              "type": "string"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Join Plan Sha256",
              "type": "string"
            },
            "join_work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Join Work Id",
              "type": "string"
            },
            "phase": {
              "const": "coordinating",
              "title": "Phase",
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "title": "Revision",
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Work Id",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase",
            "revision",
            "branch_set_sha256",
            "join_plan_sha256",
            "join_work_id"
          ],
          "title": "JoinAdmittedEventData",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/JoinAdmittedEventData"
        },
        "datacontenttype": {
          "const": "application/json",
          "default": "application/json",
          "title": "Datacontenttype",
          "type": "string"
        },
        "id": {
          "minLength": 1,
          "title": "Id",
          "type": "string"
        },
        "source": {
          "const": "urn:riverhog:stove0",
          "title": "Source",
          "type": "string"
        },
        "specversion": {
          "const": "1.0",
          "default": "1.0",
          "title": "Specversion",
          "type": "string"
        },
        "subject": {
          "minLength": 1,
          "title": "Subject",
          "type": "string"
        },
        "time": {
          "title": "Time",
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.join.admitted",
          "title": "Type",
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
      "title": "JoinAdmittedEvent",
      "type": "object"
    },
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JoinAdmittedEvent",
  "unit": "export"
}
```
