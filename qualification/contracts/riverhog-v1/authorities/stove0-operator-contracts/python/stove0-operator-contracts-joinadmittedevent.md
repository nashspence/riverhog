# stove0_operator_contracts.JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedevent:c2d272407b -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-59a3993686"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None\""`

#### Validated model schema

<a id="s-bac1e80528"></a>

- <a id="s-931ec71f9e"></a>`type`: `"object"`
- <a id="s-9fd89b828d"></a>`additionalProperties`: `false`
- <a id="s-abbe116e3a"></a>`required`: `["id","source","type","subject","time","data"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6869980525"></a>`data` | yes | [JoinAdmittedEventData](#s-1698a3ec14) |  |
| <a id="s-785e024407"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-ccb2be9233"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-51011c4491"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-d2654e8452"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-3b54a9a1b2"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-56769ad228"></a>`time` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-73450a5e5b"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted" |  |

##### Definitions

- [JoinAdmittedEventData](#s-1698a3ec14)

##### <a id="s-1698a3ec14"></a>definition `JoinAdmittedEventData`

- <a id="s-48cd7c74ff"></a>`type`: `"object"`
- <a id="s-044f374997"></a>`additionalProperties`: `false`
- <a id="s-945ba683fb"></a>`required`: `["work_id","phase","revision","branch_set_sha256","join_plan_sha256","join_work_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8d9bc76b11"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9037ba08ab"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-af028024e6"></a>`join_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad801c3f38"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-7c55bc928f"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-96832be23c"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-joinadmittedevent-exact-subject.md)

## Governing policies

- <a id="pa-729dab7ac8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 683c78d9a0ed38c6f92f4c3e1e558785fe7e8fa3dfcde8f30aa68dfe90d3e986 -->

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
              "type": "string"
            },
            "join_plan_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "join_work_id": {
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
            "join_plan_sha256",
            "join_work_id"
          ],
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
          "const": "io.riverhog.stove0.join.admitted",
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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JoinAdmittedEvent",
  "unit": "export"
}
```

</details>
