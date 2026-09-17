# stove0_operator_contracts.WorkFailureView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workfailureview:d2b84e4019 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d68b33e221"></a>
- <a id="s-88015493c5"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-03d01889e2"></a>`module`: `stove0_operator_contracts`
- <a id="s-03b57a20e3"></a>`name`: `WorkFailureView`
- <a id="s-9225fcf532"></a>`unit`: `export`

### Declared structure

- <a id="s-40a97c5085"></a>`kind`: `"class"`
- <a id="s-41bd02e099"></a>`signature`: `"'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"`

#### Validated model schema

<a id="s-93ce130c25"></a>

- <a id="s-ee10dfebec"></a>`type`: `"object"`
- <a id="s-63e1580acd"></a>`additionalProperties`: `false`
- <a id="s-2c0890897c"></a>`required`: `["code","message","retryable"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-021cefaa3e"></a>`code` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-b4638e37de"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-fbc2148b83"></a>`retryable` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-f1a759e61c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkFailureView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdc955c4506e8790ac6cb4df6592591756e12f564574e13e901c6fb21dd23ccf -->

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
        },
        "retryable": {
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "type": "object"
    },
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkFailureView",
  "unit": "export"
}
```

</details>
