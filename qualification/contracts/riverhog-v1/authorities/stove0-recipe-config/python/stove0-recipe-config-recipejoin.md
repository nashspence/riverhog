# stove0_recipe_config.RecipeJoin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipejoin:659c99f914 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae20f3e87c"></a>
- <a id="s-3b1279601b"></a>`distribution`: `stove0-recipe-config`
- <a id="s-b562b69b0c"></a>`module`: `stove0_recipe_config`
- <a id="s-5489320d70"></a>`name`: `RecipeJoin`
- <a id="s-3f431ca44b"></a>`unit`: `export`

### Declared structure

- <a id="s-5ce0dd1586"></a>`kind`: `"class"`
- <a id="s-3a6b297aee"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], members: Annotated[tuple[stove0_recipe_config.models.RecipeJoinMember, ...], MinLen(min_length=2)], operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""`

#### Validated model schema

<a id="s-d16a3c32cf"></a>

- <a id="s-1f48fb4fb8"></a>`type`: `"object"`
- <a id="s-fcbc8e2909"></a>`additionalProperties`: `false`
- <a id="s-2d7aaae763"></a>`required`: `["id","members","operation_id","target_registration_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6195bd8c6c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c23629ec46"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-feebc6fc22"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-43a3175f14)) |  |
| <a id="s-10d81d6560"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-4e893b9aee)); minItems=2 |  |
| <a id="s-3b53494533"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-713bdbd2ca"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-6d62c8e596)) |  |
| <a id="s-c492917418"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-43a3175f14)) |  |
| <a id="s-fd8185303e"></a>`target_registration_id` | yes | type="string" |  |

##### Definitions

- [JsonValue](#s-43a3175f14)
- [OperationProjection](#s-6d62c8e596)
- [RecipeJoinMember](#s-4e893b9aee)

##### <a id="s-43a3175f14"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-6d62c8e596"></a>definition `OperationProjection`

- <a id="s-3718a1ba1a"></a>`type`: `"object"`
- <a id="s-61e9809826"></a>`additionalProperties`: `false`
- <a id="s-2343a20a6a"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5552eac40a"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-83635dc2ac"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-41b1d2a430"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-27e8b67a72"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-4e893b9aee"></a>definition `RecipeJoinMember`

- <a id="s-e0eb8236d2"></a>`type`: `"object"`
- <a id="s-76828c7e74"></a>`additionalProperties`: `false`
- <a id="s-bd2a938a90"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-32584a941e"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e0be4274f0"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_projections](stove0-recipe-config-recipejoin-canonical-projections.md)

## Governing policies

- <a id="pa-1ce220b2b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeJoin`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c36eb1c60380a9d68538ce9554d45b7e8fe036dae82ba81f2672d2c7488e6885 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "OperationProjection": {
          "additionalProperties": false,
          "properties": {
            "destination": {
              "enum": [
                "intent",
                "target-options"
              ],
              "type": "string"
            },
            "destination_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            },
            "source": {
              "enum": [
                "work-effective-intent",
                "work-evaluation"
              ],
              "type": "string"
            },
            "source_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            }
          },
          "required": [
            "source",
            "source_pointer",
            "destination",
            "destination_pointer"
          ],
          "type": "object"
        },
        "RecipeJoinMember": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "input_retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/RecipeJoinMember"
          },
          "minItems": 2,
          "type": "array"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "projections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OperationProjection"
          },
          "type": "array"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        },
        "target_registration_id": {
          "type": "string"
        }
      },
      "required": [
        "id",
        "members",
        "operation_id",
        "target_registration_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], members: Annotated[tuple[stove0_recipe_config.models.RecipeJoinMember, ...], MinLen(min_length=2)], operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeJoin",
  "unit": "export"
}
```

</details>
