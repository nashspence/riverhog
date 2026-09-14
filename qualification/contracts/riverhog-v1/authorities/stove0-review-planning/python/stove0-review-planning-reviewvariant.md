# stove0_review_planning.ReviewVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-reviewvariant:6be3dc46a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1350e31c69"></a>
- <a id="s-ad57446dae"></a>`distribution`: `stove0-review-planning`
- <a id="s-2ce265330e"></a>`module`: `stove0_review_planning`
- <a id="s-7e7ebb9ff8"></a>`name`: `ReviewVariant`
- <a id="s-6f254cb7fe"></a>`unit`: `export`

### Declared structure

- <a id="s-59131906e2"></a>`kind`: `"class"`
- <a id="s-84d3614ab2"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""`

#### Validated model schema

<a id="s-119f275b32"></a>
- <a id="s-0f1a7c4977"></a>`title`: ReviewVariant
- <a id="s-fa72432407"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c33ed454d4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0b5d6934d6"></a>`portable_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-917c3968f0"></a>`target_options` | no | type="object"; additional keys=`additionalProperties` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-126b5d9bd8"></a>`JsonValue` | empty object |

## Governing policies

- <a id="pa-ebd2e1f7cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources.md#src-354ae519e9) — `reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_planning.ReviewVariant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efc258067c7dc3441dd308000b78138df908acf3c7c73e5ee88d692cbf5b3dfa -->

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
          "title": "Id",
          "type": "string"
        },
        "portable_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Portable Intent",
          "type": "object"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "id"
      ],
      "title": "ReviewVariant",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-review-planning",
  "module": "stove0_review_planning",
  "name": "ReviewVariant",
  "unit": "export"
}
```
