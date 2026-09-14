# stove0_operator_contracts.BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmittedevent:ebe45d3f32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-643e4d67ba"></a>
- <a id="s-95baea4ea7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-0aacbe8bfe"></a>`module`: `stove0_operator_contracts`
- <a id="s-9be6a4d574"></a>`name`: `BranchSetAdmittedEvent`
- <a id="s-956d10453f"></a>`unit`: `export`

### Declared structure

- <a id="s-05870c1e04"></a>`kind`: `"class"`
- <a id="s-30a03ecf14"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.branch-set.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.BranchSetAdmittedEventData) -> None\""`

#### Validated model schema

<a id="s-e283b2a5c3"></a>
- <a id="s-1a3fe35073"></a>`title`: BranchSetAdmittedEvent
- <a id="s-f41888b130"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccc3acc09a"></a>`data` | yes | #/$defs/BranchSetAdmittedEventData |  |
| <a id="s-0f6fc11c23"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-f22a2d873f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-00409b580d"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-c31ef44a03"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-a19d40f37d"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-024d0cdc1b"></a>`time` | yes | type="string" |  |
| <a id="s-3e97f83f26"></a>`type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d6a962fc6b"></a>`BranchSetAdmittedEventData` | type="object"; fields=`admitted_work_count`, `branch_count`, `branch_set_sha256`, `phase`, `revision`, `work_id`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-bab00bf4cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf2fed898a11e2c0f0310ca7a72bfde668d2f9b5e36ca88922ab3cc99345a8c3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "BranchSetAdmittedEventData": {
          "additionalProperties": false,
          "properties": {
            "admitted_work_count": {
              "minimum": 1,
              "title": "Admitted Work Count",
              "type": "integer"
            },
            "branch_count": {
              "minimum": 1,
              "title": "Branch Count",
              "type": "integer"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Branch Set Sha256",
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
            "branch_count",
            "admitted_work_count"
          ],
          "title": "BranchSetAdmittedEventData",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/BranchSetAdmittedEventData"
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
          "const": "io.riverhog.stove0.branch-set.admitted",
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
      "title": "BranchSetAdmittedEvent",
      "type": "object"
    },
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.branch-set.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.BranchSetAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BranchSetAdmittedEvent",
  "unit": "export"
}
```
