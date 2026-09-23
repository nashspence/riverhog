# review0_target_contracts.ReviewVariantIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewvariantintent:8bb940b71d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30003bbd4a"></a>
- <a id="s-cd18af6e49"></a>`distribution`: `review0-target-contracts`
- <a id="s-b36834487c"></a>`module`: `review0_target_contracts`
- <a id="s-0f3c5011ec"></a>`name`: `ReviewVariantIntent`
- <a id="s-f8c6281a9a"></a>`unit`: `export`

### Declared structure

- <a id="s-b59e7f452f"></a>`kind`: `"class"`
- <a id="s-4799b8dc4f"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-7d574e5992"></a>

- <a id="s-9ce74f00d0"></a>`type`: `"object"`
- <a id="s-e65576d7d3"></a>`additionalProperties`: `false`
- <a id="s-8bb67a4acd"></a>`required`: `["id","portable_intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6d74f1b38"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-29b863ca70"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-aae6d3312f)) |  |

##### Definitions

- [JsonValue](#s-aae6d3312f)

##### <a id="s-aae6d3312f"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-b0d919a5aa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewVariantIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 372ec58d04b75a21b1b5ff0ed60054416623453aeef7c9dfe13c584979bfce80 -->

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
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "ReviewVariantIntent",
  "unit": "export"
}
```

</details>
