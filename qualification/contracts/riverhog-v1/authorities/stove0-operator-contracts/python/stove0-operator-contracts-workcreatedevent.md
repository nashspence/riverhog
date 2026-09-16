# stove0_operator_contracts.WorkCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedevent:bb663f2571 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b09e77382"></a>
- <a id="s-e7ae0e2909"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-6df2c46ea2"></a>`module`: `stove0_operator_contracts`
- <a id="s-dfe6c0a4e6"></a>`name`: `WorkCreatedEvent`
- <a id="s-423b9c3071"></a>`unit`: `export`

### Declared structure

- <a id="s-f6c2162c8c"></a>`kind`: `"class"`
- <a id="s-7e91262c9b"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkCreatedEventData) -> None\""`

#### Validated model schema

<a id="s-8371a97cbe"></a>

- <a id="s-c2445c347d"></a>`type`: `"object"`
- <a id="s-3474d9162d"></a>`additionalProperties`: `false`
- <a id="s-e1b5134f59"></a>`required`: `["id","source","type","subject","time","data"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bb5df1c628"></a>`data` | yes | [WorkCreatedEventData](#s-6085de5cba) |  |
| <a id="s-6c65f4588c"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-2007241504"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-2c356c6a6d"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-0a6b3e077e"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-0fa46ef090"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-4e263b1025"></a>`time` | yes | type="string" |  |
| <a id="s-18839a3c6a"></a>`type` | yes | type="string"; const="io.riverhog.stove0.work.created" |  |

##### Definitions

- [WorkCreatedEventData](#s-6085de5cba)

##### <a id="s-6085de5cba"></a>definition `WorkCreatedEventData`

- <a id="s-83fc9713cc"></a>`type`: `"object"`
- <a id="s-037b2a079a"></a>`additionalProperties`: `false`
- <a id="s-ca374a66ed"></a>`required`: `["work_id","phase"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cdec4e381b"></a>`branch_set_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-6c55526b86"></a>`join_plan_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-eba0cb267a"></a>`parent_work_id` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-5ad7514f0b"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-2819be973f"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-workcreatedevent-exact-subject.md)
- [validate_time](stove0-operator-contracts-workcreatedevent-validate-time.md)

## Governing policies

- <a id="pa-8a2b43ce0a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04231be558c2017ed9b9740f2b1ea2d9038ab286c126d0125cd5a4437fad4333 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WorkCreatedEventData": {
          "additionalProperties": false,
          "properties": {
            "branch_set_sha256": {
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
            },
            "join_plan_sha256": {
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
            },
            "parent_work_id": {
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
            },
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
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "work_id",
            "phase"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/WorkCreatedEventData"
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
          "const": "io.riverhog.stove0.work.created",
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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreatedEvent",
  "unit": "export"
}
```

</details>
