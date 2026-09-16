# stove0_recipe_config.RecipeRoute

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-reciperoute:2209e0a6d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fcb824a457"></a>
- <a id="s-281b7539ec"></a>`distribution`: `stove0-recipe-config`
- <a id="s-6475798571"></a>`module`: `stove0_recipe_config`
- <a id="s-08e4b8663c"></a>`name`: `RecipeRoute`
- <a id="s-9657ea1f6b"></a>`unit`: `export`

### Declared structure

- <a id="s-b3dd60dd2e"></a>`kind`: `"class"`
- <a id="s-bc849d2fc7"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['operation'] = 'operation', operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""`

#### Validated model schema

<a id="s-e072334ab8"></a>

- <a id="s-0e3a464d9a"></a>`type`: `"object"`
- <a id="s-07f8dcc78f"></a>`additionalProperties`: `false`
- <a id="s-0dcdef9b66"></a>`required`: `["id","operation_id","target_registration_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9fe0c0c64e"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-0a5592ec8b)) |  |
| <a id="s-ffc6dc2f54"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-63ea072147"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-55877f8f06"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-b24cb9f762"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-e7e622e77a)) |  |
| <a id="s-b0d463cf4e"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-3fa53899cb"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a2d391bdf9"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-6931619c42"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-26ad3b2e40)) |  |
| <a id="s-46b394adb1"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-e7e622e77a)) |  |
| <a id="s-06d0a97041"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-186eb8ac75"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-6349fe3dd5)) |  |

##### Definitions

- [ArtifactFactBinding](#s-1f22723179)
- [ArtifactRule](#s-0a5592ec8b)
- [FactPredicate](#s-6349fe3dd5)
- [JsonValue](#s-e7e622e77a)
- [OperationProjection](#s-26ad3b2e40)

##### <a id="s-1f22723179"></a>definition `ArtifactFactBinding`

- <a id="s-8a81bf9832"></a>`type`: `"object"`
- <a id="s-050de40953"></a>`additionalProperties`: `false`
- <a id="s-f6cca8eb75"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f042fbc914"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-edae9e9acd"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-0a5592ec8b"></a>definition `ArtifactRule`

- <a id="s-44420cc80a"></a>`type`: `"object"`
- <a id="s-c5e9569865"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb84ab27b5"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-6287bbaa96"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-a2069a11e7"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-6349fe3dd5"></a>definition `FactPredicate`

- <a id="s-dfae94971f"></a>`type`: `"object"`
- <a id="s-8129e75d19"></a>`additionalProperties`: `false`
- <a id="s-03c769a01e"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ce526ddc7"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-1f22723179)); (type="null")]; default=null |  |
| <a id="s-bcc7a158eb"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-501e61feb1"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b2e941b389"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-c24e11399b"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-e4ca5f8873"></a>`value` | no | [JsonValue](#s-e7e622e77a); default=null |  |

##### <a id="s-e7e622e77a"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-26ad3b2e40"></a>definition `OperationProjection`

- <a id="s-35c9d9eb66"></a>`type`: `"object"`
- <a id="s-5b03acb7f1"></a>`additionalProperties`: `false`
- <a id="s-628a6d9e6c"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7fc8fe2f42"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-6d8ba10462"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-0f328d70b9"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-6b405dec4a"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-recipe-config-reciperoute-canonical-members.md)

## Governing policies

- <a id="pa-c3f2a2860e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeRoute`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa8761c3de0c3de9773460606bc1b573c66866e98f1047ed00c213b4346c53f7 -->

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
        "ArtifactRule": {
          "additionalProperties": false,
          "properties": {
            "glob": {
              "default": "*",
              "type": "string"
            },
            "media_type": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "role": {
              "default": "stove0.source/v1",
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "type": "object"
        },
        "FactPredicate": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "artifact_rules": {
          "default": [
            {
              "glob": "*",
              "media_type": null,
              "role": "stove0.source/v1"
            }
          ],
          "items": {
            "$ref": "#/$defs/ArtifactRule"
          },
          "type": "array"
        },
        "associated_roles": {
          "default": [],
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "type": "array"
        },
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
        "kind": {
          "const": "operation",
          "default": "operation",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "primary_role": {
          "anyOf": [
            {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
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
        },
        "when": {
          "default": [],
          "items": {
            "$ref": "#/$defs/FactPredicate"
          },
          "type": "array"
        }
      },
      "required": [
        "id",
        "operation_id",
        "target_registration_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['operation'] = 'operation', operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeRoute",
  "unit": "export"
}
```

</details>
