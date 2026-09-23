# stove0_operator_contracts.Stove0CloudEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0cloudevent:e0f8ea2258 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-efd4c54b15"></a>
- <a id="s-7648196d49"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-118e11fbe2"></a>`module`: `stove0_operator_contracts`
- <a id="s-5fdf247562"></a>`name`: `Stove0CloudEvent`
- <a id="s-51a090cd7d"></a>`unit`: `export`

### Declared structure

- <a id="s-2d9ef24fc3"></a>`kind`: `"class"`
- <a id="s-489444ae84"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: Any) -> None\""`

#### Validated model schema

<a id="s-58dccf3714"></a>

- <a id="s-7bfc9452ca"></a>`type`: `"object"`
- <a id="s-8dbe26c13f"></a>`additionalProperties`: `false`
- <a id="s-f922001099"></a>`required`: `["id","source","type","subject","time","data"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b3ad5c015c"></a>`data` | yes | any JSON value |  |
| <a id="s-12e327774b"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-7909202afe"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-503011d03a"></a>`source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| <a id="s-11497c7367"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-20c1c814af"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-cca47fbd88"></a>`time` | yes | type="string" |  |
| <a id="s-7b8dd11a65"></a>`type` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-stove0cloudevent-exact-subject.md)
- [validate_time](stove0-operator-contracts-stove0cloudevent-validate-time.md)

## Governing policies

- <a id="pa-c54ab721eb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0CloudEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ba5fa09e14aa6f6130d74d1f4b1ec321bebe73184662eb20955a1faa4e666c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "data": {},
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
          "minLength": 1,
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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: Any) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0CloudEvent",
  "unit": "export"
}
```

</details>
