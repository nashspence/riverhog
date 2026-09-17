# stove0_operator_contracts.RecipeView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-recipeview:793dfbe71d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ecc5064913"></a>
- <a id="s-4a8cfd11ec"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-1afd1bbf3c"></a>`module`: `stove0_operator_contracts`
- <a id="s-5bc2655503"></a>`name`: `RecipeView`
- <a id="s-069f123be4"></a>`unit`: `export`

### Declared structure

- <a id="s-d71cebb5d5"></a>`kind`: `"class"`
- <a id="s-01c39b3fc3"></a>`signature`: `"\"(*, definition: stove0_recipe_config.models.RecipeDefinition, sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-2a2e5bf713"></a>

- <a id="s-78b5a25a43"></a>`type`: `"object"`
- <a id="s-3be9b5fddc"></a>`additionalProperties`: `false`
- <a id="s-71278efe8b"></a>`required`: `["definition","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05a3041ee2"></a>`definition` | yes | [RecipeDefinition](#s-b1b41905a4) |  |
| <a id="s-6a4c109487"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactAssociation](#s-1868aa0b8b)
- [ArtifactFactBinding](#s-6486fe4d7c)
- [ArtifactRule](#s-6176eb75d6)
- [FactPredicate](#s-fc29a63e9d)
- [JsonValue](#s-f1f94694bd)
- [ObserverUse](#s-304ec65dcf)
- [OperationProjection](#s-cabe7839cc)
- [RecipeCoordinationRoute](#s-f7c81eefc7)
- [RecipeDefinition](#s-b1b41905a4)
- [RecipeJoin](#s-7b4c4aaca2)
- [RecipeJoinMember](#s-736ee3472b)
- [RecipeRef](#s-6f2e6802bd)
- [RecipeRoute](#s-4d9172b48a)

##### <a id="s-1868aa0b8b"></a>definition `ArtifactAssociation`

- <a id="s-5f099a07ff"></a>`type`: `"object"`
- <a id="s-e3c1f44012"></a>`additionalProperties`: `false`
- <a id="s-49f7145bbd"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d751ce781"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-bf139081be"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-ba1fc628b4"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-6486fe4d7c"></a>definition `ArtifactFactBinding`

- <a id="s-4e5920d372"></a>`type`: `"object"`
- <a id="s-85bfb7e16c"></a>`additionalProperties`: `false`
- <a id="s-b2bb1f8b96"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25ec6415a6"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-639d45249c"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-6176eb75d6"></a>definition `ArtifactRule`

- <a id="s-2275c0c462"></a>`type`: `"object"`
- <a id="s-56b8e7a3b3"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5b3a03b92"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-774763756d"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-c89aa2ac30"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-fc29a63e9d"></a>definition `FactPredicate`

- <a id="s-a9267da9c5"></a>`type`: `"object"`
- <a id="s-5a46614cd1"></a>`additionalProperties`: `false`
- <a id="s-f51e879e34"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d349a86e23"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-6486fe4d7c)); (type="null")]; default=null |  |
| <a id="s-3d7daa90f0"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-fc33e26c21"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-003bfe16aa"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-e95cbc2b4d"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-d702cc0aa0"></a>`value` | no | [JsonValue](#s-f1f94694bd); default=null |  |

##### <a id="s-f1f94694bd"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-304ec65dcf"></a>definition `ObserverUse`

- <a id="s-35d1c6ef6a"></a>`type`: `"object"`
- <a id="s-997b366a4d"></a>`additionalProperties`: `false`
- <a id="s-bf693b5788"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c184d5677a"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6176eb75d6)) |  |
| <a id="s-396a23ca00"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d0a4db076e"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-35ffbd96a3"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-be63f1266d"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-365eedf123"></a>`registration_id` | yes | type="string" |  |
| <a id="s-5e85dcd859"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-98c6e0bd7a"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-cabe7839cc"></a>definition `OperationProjection`

- <a id="s-7c1d42c51a"></a>`type`: `"object"`
- <a id="s-82278c2a74"></a>`additionalProperties`: `false`
- <a id="s-0b557bafa8"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f2c5fa667"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-0c35d01c3b"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-7de30be886"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-699e23a559"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-f7c81eefc7"></a>definition `RecipeCoordinationRoute`

- <a id="s-31f0da24b1"></a>`type`: `"object"`
- <a id="s-ce1696665f"></a>`additionalProperties`: `false`
- <a id="s-b0df17ebeb"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-343d71a77e"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6176eb75d6)) |  |
| <a id="s-ec9b3ca237"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-0e80040be0"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-72084c1854"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-e14a77fb1b"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-a2b2aac6cc"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-e55364f086"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-cabe7839cc)) |  |
| <a id="s-b742276ddd"></a>`recipe` | yes | [RecipeRef](#s-6f2e6802bd) |  |
| <a id="s-77ed3b7045"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-fc29a63e9d)) |  |

##### <a id="s-b1b41905a4"></a>definition `RecipeDefinition`

- <a id="s-2ec3919416"></a>`type`: `"object"`
- <a id="s-9eb64f1c4d"></a>`additionalProperties`: `false`
- <a id="s-9828b0d61f"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b77e1aa96"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-0a494000b0"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-1868aa0b8b)) |  |
| <a id="s-b72ac5ee71"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-7a966d6ba7"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4161c7258a"></a>`join` | no | anyOf=[([RecipeJoin](#s-7b4c4aaca2)); (type="null")]; default=null |  |
| <a id="s-748e053b7a"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-304ec65dcf)) |  |
| <a id="s-468c01acec"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-fd3e75901e"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-1b5464ebb0"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-4d9172b48a)); ([RecipeCoordinationRoute](#s-f7c81eefc7))]); minItems=1 |  |
| <a id="s-a55f23d050"></a>`source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-e24c68076e"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### <a id="s-7b4c4aaca2"></a>definition `RecipeJoin`

- <a id="s-5e2e11d237"></a>`type`: `"object"`
- <a id="s-164e6d85fa"></a>`additionalProperties`: `false`
- <a id="s-5126aa7309"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9df7fcd883"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b5594f7232"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3701547c8f"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-9d9a29e5f2"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-736ee3472b)); minItems=2 |  |
| <a id="s-d8200f65b6"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a1f7155410"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-cabe7839cc)) |  |
| <a id="s-043b712be3"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-d0b169ac60"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-736ee3472b"></a>definition `RecipeJoinMember`

- <a id="s-a9d386e90f"></a>`type`: `"object"`
- <a id="s-b256b3d69a"></a>`additionalProperties`: `false`
- <a id="s-72f6350f85"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74cf08a4fa"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c5c2172a7f"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-6f2e6802bd"></a>definition `RecipeRef`

- <a id="s-b0c5b2d91b"></a>`type`: `"object"`
- <a id="s-01a0f08ace"></a>`additionalProperties`: `false`
- <a id="s-5d31ec7405"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9baf0b741c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d67928f034"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-a44ec1acbc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-4d9172b48a"></a>definition `RecipeRoute`

- <a id="s-2d89d0fb89"></a>`type`: `"object"`
- <a id="s-f2b09ff792"></a>`additionalProperties`: `false`
- <a id="s-f6ceba21f4"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4f50d27119"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6176eb75d6)) |  |
| <a id="s-d1565106da"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-5d479732b2"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3c0cd87d0f"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-c57a2a111a"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-15ae3d5fb0"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-05fe5bc9cf"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2a2302d64a"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-ab4e2063fd"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-cabe7839cc)) |  |
| <a id="s-b72eacf20a"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-f1f94694bd)) |  |
| <a id="s-8fbec20326"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-6a427e4377"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-fc29a63e9d)) |  |

## Maintained corroboration

### Related interface records

- [exact_digest](stove0-operator-contracts-recipeview-exact-digest.md)
- [from_definition](stove0-operator-contracts-recipeview-from-definition.md)

## Governing policies

- <a id="pa-97ffd5ad4f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.RecipeView`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 128d5af7e94e4f571a5eb2da24b3ca7c6dd3b70546b27a20f6024963ccb99451 -->

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
        }
      },
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
    },
    "signature": "\"(*, definition: stove0_recipe_config.models.RecipeDefinition, sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "RecipeView",
  "unit": "export"
}
```

</details>
