# stove0_operator_contracts.RecipeCatalogView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipecatalogview:fd87349113 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b818e8959"></a>
- <a id="s-6d179e3636"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-997203dcf0"></a>`module`: `stove0_operator_contracts`
- <a id="s-b83e24cb66"></a>`name`: `RecipeCatalogView`
- <a id="s-045247b0e7"></a>`unit`: `export`

### Declared structure

- <a id="s-41b7b8ab06"></a>`kind`: `"class"`
- <a id="s-8d9ae17af6"></a>`signature`: `"\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None\""`

#### Validated model schema

<a id="s-5377ba1876"></a>

- <a id="s-16743d2ca5"></a>`type`: `"object"`
- <a id="s-6131011170"></a>`additionalProperties`: `false`
- <a id="s-900e88c60d"></a>`required`: `["catalog_sha256","recipes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b0fd7d84d2"></a>`catalog_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2972f443c9"></a>`recipes` | yes | type="array"; items=([RecipeView](#s-956c0286d2)) |  |

##### Definitions

- [ArtifactAssociation](#s-225da6e177)
- [ArtifactFactBinding](#s-1391b3e97b)
- [ArtifactRule](#s-918af1412c)
- [FactPredicate](#s-191cf3054d)
- [JsonValue](#s-1193b89a7e)
- [ObserverUse](#s-24ff768245)
- [OperationProjection](#s-65e7d01ae2)
- [RecipeCoordinationRoute](#s-8e40bd974f)
- [RecipeDefinition](#s-efb3ec8bd4)
- [RecipeJoin](#s-1fbcbf5a4b)
- [RecipeJoinMember](#s-a728e834ee)
- [RecipeRef](#s-f2e61bf2dd)
- [RecipeRoute](#s-26e5942589)
- [RecipeView](#s-956c0286d2)

##### <a id="s-225da6e177"></a>definition `ArtifactAssociation`

- <a id="s-c817ef51af"></a>`type`: `"object"`
- <a id="s-a173ad971d"></a>`additionalProperties`: `false`
- <a id="s-949852d49f"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9775831760"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-57e155d6ee"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-e66c53df19"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-1391b3e97b"></a>definition `ArtifactFactBinding`

- <a id="s-3ed1efc11d"></a>`type`: `"object"`
- <a id="s-e7fc5f7d0b"></a>`additionalProperties`: `false`
- <a id="s-a49ba846b7"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-602d6cb6b6"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-1137d6dac2"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-918af1412c"></a>definition `ArtifactRule`

- <a id="s-ca7c3eb588"></a>`type`: `"object"`
- <a id="s-0187ff51a2"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac3a5dc739"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-228f8f3132"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-18ba767908"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-191cf3054d"></a>definition `FactPredicate`

- <a id="s-4785f45bec"></a>`type`: `"object"`
- <a id="s-4248aaa83b"></a>`additionalProperties`: `false`
- <a id="s-9a3a5c4f60"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d22af060a1"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-1391b3e97b)); (type="null")]; default=null |  |
| <a id="s-6e23791a1f"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-e00b1310b9"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e3f3ae69ff"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-5b78b52090"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-82860b268d"></a>`value` | no | [JsonValue](#s-1193b89a7e); default=null |  |

##### <a id="s-1193b89a7e"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-24ff768245"></a>definition `ObserverUse`

- <a id="s-7ff5e7aa76"></a>`type`: `"object"`
- <a id="s-83e92f9a0c"></a>`additionalProperties`: `false`
- <a id="s-831dec8ba4"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-35743ba18f"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-918af1412c)) |  |
| <a id="s-8a080f3762"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f299ea539c"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5dc179d185"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-425c084d73"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-ea22c8c652"></a>`registration_id` | yes | type="string" |  |
| <a id="s-609e9de492"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-591961b60e"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-65e7d01ae2"></a>definition `OperationProjection`

- <a id="s-8d7af5e176"></a>`type`: `"object"`
- <a id="s-735a5b7234"></a>`additionalProperties`: `false`
- <a id="s-571f823488"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6068e0faba"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-7641eace37"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-7ccb0f5eba"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-e46cf2044c"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-8e40bd974f"></a>definition `RecipeCoordinationRoute`

- <a id="s-65f4a99abc"></a>`type`: `"object"`
- <a id="s-61e3ec166d"></a>`additionalProperties`: `false`
- <a id="s-d8c168617a"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7036e6d7ca"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-918af1412c)) |  |
| <a id="s-47810a915a"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-326c35346a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-8ed2ec8c9d"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-a448867c2d"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-b0f2a34313"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-f5cab4d14a"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-65e7d01ae2)) |  |
| <a id="s-d6f1a50642"></a>`recipe` | yes | [RecipeRef](#s-f2e61bf2dd) |  |
| <a id="s-af829c17f5"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-191cf3054d)) |  |

##### <a id="s-efb3ec8bd4"></a>definition `RecipeDefinition`

- <a id="s-d2f8d36295"></a>`type`: `"object"`
- <a id="s-f4a2338174"></a>`additionalProperties`: `false`
- <a id="s-043b144024"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2286f4f6ba"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-950d448a83"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-225da6e177)) |  |
| <a id="s-e4836c8630"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-c4d29676aa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-239d6f2f42"></a>`join` | no | anyOf=[([RecipeJoin](#s-1fbcbf5a4b)); (type="null")]; default=null |  |
| <a id="s-88c46b7fff"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-24ff768245)) |  |
| <a id="s-3af47a4cb0"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-2e609aa0e9"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-f680c7086f"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-26e5942589)); ([RecipeCoordinationRoute](#s-8e40bd974f))]); minItems=1 |  |
| <a id="s-dcdfd1c734"></a>`source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-35e31a71e5"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### <a id="s-1fbcbf5a4b"></a>definition `RecipeJoin`

- <a id="s-a6de06ed1f"></a>`type`: `"object"`
- <a id="s-309fd43562"></a>`additionalProperties`: `false`
- <a id="s-caae4dc6d8"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66efe20d24"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0f99ca9e9a"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-8e5a6386d1"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-3d9116cd12"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-a728e834ee)); minItems=2 |  |
| <a id="s-a34106fc6e"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-85115fa1b8"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-65e7d01ae2)) |  |
| <a id="s-acfe86074b"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-bedfe247f3"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-a728e834ee"></a>definition `RecipeJoinMember`

- <a id="s-76ac86c5ce"></a>`type`: `"object"`
- <a id="s-c68c03bc16"></a>`additionalProperties`: `false`
- <a id="s-27a579aba2"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b957db952c"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-154b3953ff"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-f2e61bf2dd"></a>definition `RecipeRef`

- <a id="s-b45e89606b"></a>`type`: `"object"`
- <a id="s-07c48107f9"></a>`additionalProperties`: `false`
- <a id="s-7abb60538a"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9262c35a9a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c434954ba6"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-a17fad8887"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-26e5942589"></a>definition `RecipeRoute`

- <a id="s-e2ece28680"></a>`type`: `"object"`
- <a id="s-70ddbe1f52"></a>`additionalProperties`: `false`
- <a id="s-f099c4c3db"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb146762d1"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-918af1412c)) |  |
| <a id="s-86be421e71"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-f194caa35b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b0eb900fb5"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-942f1a0771"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-1dd388bd5d"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-83ad08347f"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-81003438e2"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-fb1e461895"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-65e7d01ae2)) |  |
| <a id="s-3f3b18430a"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-1193b89a7e)) |  |
| <a id="s-6f2764e2d7"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-ef92d08762"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-191cf3054d)) |  |

##### <a id="s-956c0286d2"></a>definition `RecipeView`

- <a id="s-9d0aa0c0a7"></a>`type`: `"object"`
- <a id="s-42bd0223da"></a>`additionalProperties`: `false`
- <a id="s-e8e11e64e1"></a>`required`: `["definition","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-769372864e"></a>`definition` | yes | [RecipeDefinition](#s-efb3ec8bd4) |  |
| <a id="s-5e1c09265c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-40779a14ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeCatalogView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c47c73bea4455a84f48f37202816b55bbfbe647bcf058a71cd2d4e44ccc69c46 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactAssociation": {
          "additionalProperties": false,
          "properties": {
            "associated_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "path_identity": {
              "const": "same-parent-stem",
              "default": "same-parent-stem",
              "type": "string"
            },
            "primary_role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "primary_role",
            "associated_roles"
          ],
          "type": "object"
        },
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
        "ObserverUse": {
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
            "contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "registration_id": {
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "registration_id",
            "contract_id",
            "contract_sha256"
          ],
          "type": "object"
        },
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
        "RecipeCoordinationRoute": {
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
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
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
            "recipe": {
              "$ref": "#/$defs/RecipeRef"
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
            "recipe"
          ],
          "type": "object"
        },
        "RecipeDefinition": {
          "additionalProperties": false,
          "properties": {
            "allow_derived_inputs": {
              "default": false,
              "type": "boolean"
            },
            "artifact_associations": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ArtifactAssociation"
              },
              "type": "array"
            },
            "event_input_closure": {
              "const": "single-finalized-collection",
              "default": "single-finalized-collection",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "join": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RecipeJoin"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "observers": {
              "default": [],
              "items": {
                "$ref": "#/$defs/ObserverUse"
              },
              "type": "array"
            },
            "retirement_grace_seconds": {
              "default": 0,
              "minimum": 0,
              "type": "integer"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            },
            "routes": {
              "items": {
                "discriminator": {
                  "mapping": {
                    "coordination": "#/$defs/RecipeCoordinationRoute",
                    "operation": "#/$defs/RecipeRoute"
                  },
                  "propertyName": "kind"
                },
                "oneOf": [
                  {
                    "$ref": "#/$defs/RecipeRoute"
                  },
                  {
                    "$ref": "#/$defs/RecipeCoordinationRoute"
                  }
                ]
              },
              "minItems": 1,
              "type": "array"
            },
            "source_retirement_policy": {
              "default": "retain",
              "enum": [
                "retain",
                "retire-after-verified-output"
              ],
              "type": "string"
            },
            "unmatched_artifact_disposition": {
              "enum": [
                "retain-in-source",
                "reject-work"
              ],
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "routes",
            "unmatched_artifact_disposition"
          ],
          "type": "object"
        },
        "RecipeJoin": {
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
        },
        "RecipeRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "minimum": 1,
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        },
        "RecipeRoute": {
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
        "RecipeView": {
          "additionalProperties": false,
          "properties": {
            "definition": {
              "$ref": "#/$defs/RecipeDefinition"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "definition",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "catalog_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "recipes": {
          "items": {
            "$ref": "#/$defs/RecipeView"
          },
          "type": "array"
        }
      },
      "required": [
        "catalog_sha256",
        "recipes"
      ],
      "type": "object"
    },
    "signature": "\"(*, catalog_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], recipes: tuple[stove0_operator_contracts.RecipeView, ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "RecipeCatalogView",
  "unit": "export"
}
```

</details>
