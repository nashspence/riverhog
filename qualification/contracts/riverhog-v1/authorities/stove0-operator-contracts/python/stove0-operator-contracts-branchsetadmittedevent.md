# stove0_operator_contracts.BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-branchsetadmittedevent:ebe45d3f32 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-f41888b130"></a>`type`: `"object"`
- <a id="s-ebe041e116"></a>`additionalProperties`: `false`
- <a id="s-0a9fe158b9"></a>`required`: `["id","source","type","subject","time","data"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ccc3acc09a"></a>`data` | yes | [BranchSetAdmittedEventData](#s-d6a962fc6b) |  |
| <a id="s-0f6fc11c23"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-f22a2d873f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-00409b580d"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-c31ef44a03"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-a19d40f37d"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-024d0cdc1b"></a>`time` | yes | type="string" |  |
| <a id="s-3e97f83f26"></a>`type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted" |  |

##### Definitions

- [BranchSetAdmittedEventData](#s-d6a962fc6b)

##### <a id="s-d6a962fc6b"></a>definition `BranchSetAdmittedEventData`

- <a id="s-836e182505"></a>`type`: `"object"`
- <a id="s-95d68e8684"></a>`additionalProperties`: `false`
- <a id="s-08e68e0093"></a>`required`: `["work_id","phase","revision","branch_set_sha256","branch_count","admitted_work_count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-201f0a3187"></a>`admitted_work_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-ea78a957eb"></a>`branch_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-60611ab00a"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3bb511aec1"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-0800b318fb"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-3676a3d00b"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-branchsetadmittedevent-exact-subject.md)
- [validate_time](stove0-operator-contracts-branchsetadmittedevent-validate-time.md)

## Governing policies

- <a id="pa-bab00bf4cd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.BranchSetAdmittedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d74d7f4c0a7add424ab5b70d773ab426d16543042344946f8907ebbed3b9f4cf -->

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
              "type": "integer"
            },
            "branch_count": {
              "minimum": 1,
              "type": "integer"
            },
            "branch_set_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "const": "coordinating",
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
            "revision",
            "branch_set_sha256",
            "branch_count",
            "admitted_work_count"
          ],
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
          "const": "io.riverhog.stove0.branch-set.admitted",
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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.branch-set.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.BranchSetAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "BranchSetAdmittedEvent",
  "unit": "export"
}
```

</details>
