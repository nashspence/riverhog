# stove0_operator_contracts.WorkClaimView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workclaimview:b7ed8b9383 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05e377cda1"></a>
- <a id="s-f2abc50185"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-bcd9d42f07"></a>`module`: `stove0_operator_contracts`
- <a id="s-eacfc19fc2"></a>`name`: `WorkClaimView`
- <a id="s-b21cf564b6"></a>`unit`: `export`

### Declared structure

- <a id="s-9a0ef7733d"></a>`kind`: `"class"`
- <a id="s-aa929b9ee3"></a>`signature`: `"'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-02ff3ec1ea"></a>

- <a id="s-823b3558e0"></a>`type`: `"object"`
- <a id="s-efe99d8bf7"></a>`additionalProperties`: `false`
- <a id="s-0f90dba874"></a>`required`: `["claim_id","fence"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf24e8d350"></a>`claim_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-4b7161e96f"></a>`fence` | yes | type="integer"; minimum=1 |  |

## Governing policies

- <a id="pa-968ca8b933"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkClaimView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb7f3b086f1ba7339aa1933185c001eac294e1ce5b0cdc6fe188cd168a2f69ef -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "claim_id",
        "fence"
      ],
      "type": "object"
    },
    "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkClaimView",
  "unit": "export"
}
```

</details>
