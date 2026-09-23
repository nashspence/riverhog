# review0_planner.ReviewVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-planner:review0-planner-reviewvariant:5b3b6329b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-planner](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c84a4979b"></a>
- <a id="s-cfa32222ab"></a>`distribution`: `review0-planner`
- <a id="s-5ea5390863"></a>`module`: `review0_planner`
- <a id="s-a4929382c0"></a>`name`: `ReviewVariant`
- <a id="s-95a03705c5"></a>`unit`: `export`

### Declared structure

- <a id="s-899f272dad"></a>`kind`: `"class"`
- <a id="s-9f0549344e"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-26823fbb3f"></a>

- <a id="s-cc7b888be2"></a>`type`: `"object"`
- <a id="s-ba8689cf7a"></a>`additionalProperties`: `false`
- <a id="s-9063eaec8c"></a>`required`: `["id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-169df14342"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b4bc6bdff3"></a>`portable_intent` | no | type="object"; additionalProperties=([JsonValue](#s-72e7ae31e7)) |  |
| <a id="s-4b9ea6607c"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-72e7ae31e7)) |  |

##### Definitions

- [JsonValue](#s-72e7ae31e7)

##### <a id="s-72e7ae31e7"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-95c5e8a255"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-planner:review0_planner](../../../evidence/sources/authorities.md#src-a8a843c072) — [some-implementations/stove0/review0/planning/src/review0\_planner/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/planning/src/review0_planner/__init__.py)

### Machine authority

- `/external_contract/python/review0_planner.ReviewVariant`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 312081f479a7e841a7a58c8db5649c233d7cb7745902d26ecdc0803498cb18bd -->

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
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        }
      },
      "required": [
        "id"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "review0-planner",
  "module": "review0_planner",
  "name": "ReviewVariant",
  "unit": "export"
}
```

</details>
