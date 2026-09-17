# stove0_operator_contracts.WorkInapplicableView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workinapplicableview:61ad7fcd62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4459cf90ae"></a>
- <a id="s-5ca4613920"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-7fdfdcd881"></a>`module`: `stove0_operator_contracts`
- <a id="s-ed2e15bd58"></a>`name`: `WorkInapplicableView`
- <a id="s-8abf7d6945"></a>`unit`: `export`

### Declared structure

- <a id="s-26b196ed56"></a>`kind`: `"class"`
- <a id="s-cdb4a41092"></a>`signature`: `"'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"`

#### Validated model schema

<a id="s-50699efa8d"></a>

- <a id="s-f8d3e81982"></a>`type`: `"object"`
- <a id="s-08a218baf6"></a>`additionalProperties`: `false`
- <a id="s-0877d5d2a1"></a>`required`: `["code","message"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-96f1dd28c2"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-28e85e685b"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

## Governing policies

- <a id="pa-5f1f4765b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkInapplicableView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92833e1bf8484b16b3a53e44deeccc901219090115b8e740b976ac155d752307 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkInapplicableView",
  "unit": "export"
}
```

</details>
