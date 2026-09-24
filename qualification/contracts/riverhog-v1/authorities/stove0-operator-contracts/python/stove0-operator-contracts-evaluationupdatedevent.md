# stove0_operator_contracts.EvaluationUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdatedevent:8564549796 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-062f7eb7ef"></a>
- <a id="s-5ccc6043b3"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-19ba929f51"></a>`module`: `stove0_operator_contracts`
- <a id="s-fd39019df9"></a>`name`: `EvaluationUpdatedEvent`
- <a id="s-d58988d1b3"></a>`unit`: `export`

### Declared structure

- <a id="s-ef44a085a3"></a>`kind`: `"class"`
- <a id="s-5b6d3438ad"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.updated'], subject: Annotated[str, MinLen(min_length=1)], time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationUpdatedEventData) -> None\""`

#### Validated model schema

<a id="s-2b454f576a"></a>

- <a id="s-3ba4ded61b"></a>`type`: `"object"`
- <a id="s-3c798a40a9"></a>`additionalProperties`: `false`
- <a id="s-bba09db829"></a>`required`: `["id","source","type","subject","time","data"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a6a8e49a71"></a>`data` | yes | [EvaluationUpdatedEventData](#s-267e631fd0) |  |
| <a id="s-dbcd58ed46"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-34a9bd9582"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-def10d38ca"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-6a3681d5ee"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-06a722e14d"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-27203fd068"></a>`time` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-f4794df724"></a>`type` | yes | type="string"; const="io.riverhog.stove0.evaluation.updated" |  |

##### Definitions

- [EvaluationUpdatedEventData](#s-267e631fd0)

##### <a id="s-267e631fd0"></a>definition `EvaluationUpdatedEventData`

- <a id="s-91ae55640d"></a>`type`: `"object"`
- <a id="s-ce31225fc1"></a>`additionalProperties`: `false`
- <a id="s-742527ca40"></a>`required`: `["evaluation_id","phase","revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c81b1a0f7"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23b7439dc4"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-637aff540a"></a>`revision` | yes | type="integer"; minimum=2 |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-evaluationupdatedevent-exact-subject.md)

## Governing policies

- <a id="pa-3f67e8b575"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ddb38b27593213d6b4ce72bf7423998e1f61fad3cde522c67baa3d1e108ca14b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "EvaluationUpdatedEventData": {
          "additionalProperties": false,
          "properties": {
            "evaluation_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "phase": {
              "enum": [
                "planning",
                "running",
                "partially_complete",
                "complete",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "revision": {
              "minimum": 2,
              "type": "integer"
            }
          },
          "required": [
            "evaluation_id",
            "phase",
            "revision"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "data": {
          "$ref": "#/$defs/EvaluationUpdatedEventData"
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        "type": {
          "const": "io.riverhog.stove0.evaluation.updated",
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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.updated'], subject: Annotated[str, MinLen(min_length=1)], time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationUpdatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationUpdatedEvent",
  "unit": "export"
}
```

</details>
