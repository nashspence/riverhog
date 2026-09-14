# stove0_review_target_contracts.ReviewVariantIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewvariantintent:69ff991f2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ffae67ccc1"></a>
- <a id="s-6ef4539618"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-11f199b36d"></a>`module`: `stove0_review_target_contracts`
- <a id="s-396f300eb6"></a>`name`: `ReviewVariantIntent`
- <a id="s-6b17f90506"></a>`unit`: `export`

### Declared structure

- <a id="s-48323d2955"></a>`kind`: `"class"`
- <a id="s-15ac1ac2d2"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-666bc3eeb8"></a>
- <a id="s-9815c3e42e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59bea9f38b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4348b9ab56"></a>`portable_intent` | yes | type="object"; additional keys=`additionalProperties` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-c0ce23da39"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-062aea031f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewVariantIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d1eb56c7a3cc29085c740adde24a95b7b5c9f16dd4c476b92849434d0283364 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "portable_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        }
      },
      "required": [
        "id",
        "portable_intent"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewVariantIntent",
  "unit": "export"
}
```
