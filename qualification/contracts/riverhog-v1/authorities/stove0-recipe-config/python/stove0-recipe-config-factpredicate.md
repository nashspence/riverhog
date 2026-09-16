# stove0_recipe_config.FactPredicate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-factpredicate:7c6bd52834 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3a0437ad8"></a>
- <a id="s-fb3c031c1f"></a>`distribution`: `stove0-recipe-config`
- <a id="s-008ced19c2"></a>`module`: `stove0_recipe_config`
- <a id="s-be3f48f4cc"></a>`name`: `FactPredicate`
- <a id="s-163e3a8f4c"></a>`unit`: `export`

### Declared structure

- <a id="s-677ea46ccb"></a>`kind`: `"class"`
- <a id="s-23402314d5"></a>`signature`: `"\"(*, observation_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), artifact_facts: stove0_recipe_config.models.ArtifactFactBinding \| None = None, pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$')], operator: Literal['equals', 'not-equals', 'contains', 'exists'] = 'equals', value: JsonValue = None) -> None\""`

#### Validated model schema

<a id="s-a5ac0fd8fb"></a>

- <a id="s-59f1afc9f5"></a>`type`: `"object"`
- <a id="s-d5c000c7cc"></a>`additionalProperties`: `false`
- <a id="s-c9ee68895e"></a>`required`: `["observation_contract_id","pointer"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-367d96c609"></a>`artifact_facts` | no | anyOf=([ArtifactFactBinding](#s-48aaf43fde)) \| (type="null"); default=null |  |
| <a id="s-6e947f2410"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-ba4b9c8c2f"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2699482742"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-ed01adb3ef"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-8547d3377b"></a>`value` | no | [JsonValue](#s-8d3fc6fcd3); default=null |  |

##### Definitions

- [ArtifactFactBinding](#s-48aaf43fde)
- [JsonValue](#s-8d3fc6fcd3)

##### <a id="s-48aaf43fde"></a>definition `ArtifactFactBinding`

- <a id="s-2f919cd848"></a>`type`: `"object"`
- <a id="s-2d6eed4ec9"></a>`additionalProperties`: `false`
- <a id="s-f78d5ba259"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b6fb4a2028"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-f9c19f23fc"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-8d3fc6fcd3"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [valid_scope](stove0-recipe-config-factpredicate-valid-scope.md)

## Governing policies

- <a id="pa-a7b6668dba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.FactPredicate`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2a76b19b4bdac139debe0ba3d052906b216175397603af1aa4739e568a4e194 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactFactBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_id_pointer": {
              "default": "/artifact_id",
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            },
            "records_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            }
          },
          "required": [
            "records_pointer"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "artifact_facts": {
          "anyOf": [
            {
              "$ref": "#/$defs/ArtifactFactBinding"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "artifact_roles": {
          "default": [],
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
        "observation_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "operator": {
          "default": "equals",
          "enum": [
            "equals",
            "not-equals",
            "contains",
            "exists"
          ],
          "type": "string"
        },
        "pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "type": "string"
        },
        "value": {
          "$ref": "#/$defs/JsonValue",
          "default": null
        }
      },
      "required": [
        "observation_contract_id",
        "pointer"
      ],
      "type": "object"
    },
    "signature": "\"(*, observation_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), artifact_facts: stove0_recipe_config.models.ArtifactFactBinding | None = None, pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], operator: Literal['equals', 'not-equals', 'contains', 'exists'] = 'equals', value: JsonValue = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "FactPredicate",
  "unit": "export"
}
```

</details>
