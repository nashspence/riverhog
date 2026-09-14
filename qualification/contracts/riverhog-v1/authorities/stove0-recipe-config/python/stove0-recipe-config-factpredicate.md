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
- <a id="s-fdf411e4c2"></a>`title`: FactPredicate
- <a id="s-59f1afc9f5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-367d96c609"></a>`artifact_facts` | no | anyOf=#/$defs/ArtifactFactBinding \| type="null" |  |
| <a id="s-6e947f2410"></a>`artifact_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-ba4b9c8c2f"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2699482742"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"] |  |
| <a id="s-ed01adb3ef"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-8547d3377b"></a>`value` | no | $ref="#/$defs/JsonValue" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-48aaf43fde"></a>`ArtifactFactBinding` | type="object"; fields=`artifact_id_pointer`, `records_pointer`; additional keys=`additionalProperties`, `required` |
| <a id="s-8d3fc6fcd3"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.FactPredicate.valid_scope](stove0-recipe-config-factpredicate-valid-scope.md)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4d99d9f35dccdafce7fbe76e7930b7a1c843b58b18102803d480a549cd754df -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactFactBinding": {
          "additionalProperties": false,
          "description": "Locate subject-keyed records inside one observer's declared facts schema.",
          "properties": {
            "artifact_id_pointer": {
              "default": "/artifact_id",
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "title": "Artifact Id Pointer",
              "type": "string"
            },
            "records_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "title": "Records Pointer",
              "type": "string"
            }
          },
          "required": [
            "records_pointer"
          ],
          "title": "ArtifactFactBinding",
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
          "title": "Artifact Roles",
          "type": "array"
        },
        "observation_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observation Contract Id",
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
          "title": "Operator",
          "type": "string"
        },
        "pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Pointer",
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
      "title": "FactPredicate",
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
