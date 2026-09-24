# stove0_core.RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition:0edcbcb7e7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45957da6ae"></a>
- <a id="s-9e4b232672"></a>`distribution`: `stove0-server`
- <a id="s-a611db797e"></a>`module`: `stove0_core`
- <a id="s-d1b48c83db"></a>`name`: `RecipeDefinition`
- <a id="s-3f1f7951bc"></a>`unit`: `export`

### Declared structure

- <a id="s-25cbeb15ae"></a>`kind`: `"class"`
- <a id="s-15abc1659d"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute \| stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin \| None = None) -> None\""`

#### Validated model schema

<a id="s-3371093a0b"></a>

- <a id="s-01899ef6e9"></a>`type`: `"object"`
- <a id="s-deafd759bd"></a>`additionalProperties`: `false`
- <a id="s-3b914cf071"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-56b9e79962"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-1c97242948"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-80f7a8b2e1)) |  |
| <a id="s-40cc1c213f"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-a8c64ce91a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d6604860df"></a>`join` | no | anyOf=[([RecipeJoin](#s-2e873f70e1)); (type="null")]; default=null |  |
| <a id="s-4f80cada1c"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-b06f9634b7)) |  |
| <a id="s-7433435a47"></a>`revision` | yes | [NonnegativeDecimal](#s-323d7d176e); ge=1 |  |
| <a id="s-1946289bd0"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-955be0c065)); ([RecipeCoordinationRoute](#s-3accea3610))]); minItems=1 |  |
| <a id="s-d43d9709cb"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-e90587bdc6"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-42fa3acc9f"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### Definitions

- [ArtifactAssociation](#s-80f7a8b2e1)
- [ArtifactFactBinding](#s-96a56437c4)
- [ArtifactRule](#s-020f8a6c73)
- [FactPredicate](#s-b67d93d394)
- [JsonValue](#s-67c0d99cde)
- [NonnegativeDecimal](#s-323d7d176e)
- [ObserverUse](#s-b06f9634b7)
- [OperationProjection](#s-5744742ffe)
- [RecipeCoordinationRoute](#s-3accea3610)
- [RecipeIdentityRef](#s-a3a502e8db)
- [RecipeJoin](#s-2e873f70e1)
- [RecipeJoinMember](#s-b94c1be1a2)
- [RecipeRoute](#s-955be0c065)

##### <a id="s-80f7a8b2e1"></a>definition `ArtifactAssociation`

- <a id="s-9b73f47a19"></a>`type`: `"object"`
- <a id="s-e8b128eaac"></a>`additionalProperties`: `false`
- <a id="s-3563146cf8"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dbc1758b9"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-271688adb9"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-72d705d06e"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-96a56437c4"></a>definition `ArtifactFactBinding`

- <a id="s-5a9a684ff7"></a>`type`: `"object"`
- <a id="s-5248696ec8"></a>`additionalProperties`: `false`
- <a id="s-ae3badef17"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a51dee8186"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-fd0c3127bb"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-020f8a6c73"></a>definition `ArtifactRule`

- <a id="s-c9f33d1460"></a>`type`: `"object"`
- <a id="s-0c2c0925fe"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-93ebe9b2a6"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-e9ed38f961"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-cf16a3f0ee"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-b67d93d394"></a>definition `FactPredicate`

- <a id="s-feee699d40"></a>`type`: `"object"`
- <a id="s-a5bd080e59"></a>`additionalProperties`: `false`
- <a id="s-5f91b4f6ab"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9672ba29cb"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-96a56437c4)); (type="null")]; default=null |  |
| <a id="s-3e66ef8600"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-23adbf5aab"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-858fa6607b"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-db639a3e78"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-0ed2d16ec8"></a>`value` | no | [JsonValue](#s-67c0d99cde); default=null |  |

##### <a id="s-67c0d99cde"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-323d7d176e"></a>definition `NonnegativeDecimal`

- <a id="s-371d7c4fd2"></a>`type`: `"string"`
- <a id="s-318b70cf1d"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-b06f9634b7"></a>definition `ObserverUse`

- <a id="s-659aad5d83"></a>`type`: `"object"`
- <a id="s-bd70f726b7"></a>`additionalProperties`: `false`
- <a id="s-ce27e8b4b9"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb02f7dd52"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-020f8a6c73)) |  |
| <a id="s-7a0f7c0ad6"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fc0828c525"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b7e6ebb68d"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-4c42e5d794"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-a5986b7c67"></a>`registration_id` | yes | type="string" |  |
| <a id="s-9fe08ba9e2"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-be53a5f391"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-5744742ffe"></a>definition `OperationProjection`

- <a id="s-db1ee3b9c4"></a>`type`: `"object"`
- <a id="s-3edaf85981"></a>`additionalProperties`: `false`
- <a id="s-7b5ba69266"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd96a17327"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-f4970f5989"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-5fd1e5df7c"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-fcf34519df"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-3accea3610"></a>definition `RecipeCoordinationRoute`

- <a id="s-86588e6811"></a>`type`: `"object"`
- <a id="s-090ad3ef0d"></a>`additionalProperties`: `false`
- <a id="s-a8fee2da9a"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be137f7af8"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-020f8a6c73)) |  |
| <a id="s-e3e820ab92"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-1685a12e07"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dcc0c384cd"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-76f5510eb7"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-6306987a9b"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-7c0951b6ae"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5744742ffe)) |  |
| <a id="s-0427a1a4be"></a>`recipe` | yes | [RecipeIdentityRef](#s-a3a502e8db) |  |
| <a id="s-feda212138"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-b67d93d394)) |  |

##### <a id="s-a3a502e8db"></a>definition `RecipeIdentityRef`

- <a id="s-585f7b1277"></a>`type`: `"object"`
- <a id="s-95af234a12"></a>`additionalProperties`: `false`
- <a id="s-0f7a33c79d"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2e15e8a0ba"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ab08aab6df"></a>`revision` | yes | [NonnegativeDecimal](#s-323d7d176e); ge=1 |  |
| <a id="s-554bd77f5d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-2e873f70e1"></a>definition `RecipeJoin`

- <a id="s-8dce64a5f9"></a>`type`: `"object"`
- <a id="s-37e8136fb6"></a>`additionalProperties`: `false`
- <a id="s-8846e1b221"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cf1055a99"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-975954e0b5"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-1a79cc8e6a"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-848fd35b80"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-b94c1be1a2)); minItems=2 |  |
| <a id="s-c6e72be79b"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d7cf417a44"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5744742ffe)) |  |
| <a id="s-48315f156b"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-30531f43db"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-b94c1be1a2"></a>definition `RecipeJoinMember`

- <a id="s-20718b1bc3"></a>`type`: `"object"`
- <a id="s-4137419e42"></a>`additionalProperties`: `false`
- <a id="s-68f78293c3"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5facbeb7ae"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d7e300bd97"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-955be0c065"></a>definition `RecipeRoute`

- <a id="s-c9907be594"></a>`type`: `"object"`
- <a id="s-b186834d15"></a>`additionalProperties`: `false`
- <a id="s-c74d7a82cd"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6dab1f9bb7"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-020f8a6c73)) |  |
| <a id="s-5fce2d13ec"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-2aee642e56"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bd756617f9"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-64326ea29d"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-81a8f098cc"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-6bbf8b2162"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d33925228c"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-0deb46aeb5"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5744742ffe)) |  |
| <a id="s-2e2e146785"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-67c0d99cde)) |  |
| <a id="s-88918b4111"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-a5910e75d6"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-b67d93d394)) |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-core-recipedefinition-canonical-members.md)
- [identity_document](stove0-core-recipedefinition-identity-document.md)
- [ref](stove0-core-recipedefinition-ref.md)
- [sha256](stove0-core-recipedefinition-sha256.md)

## Governing policies

- <a id="pa-68986ec42f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1119f9d85e62f5334684662bbdebc2fab28f5d97823b35809201fb1e96b1684 -->

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
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
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
              "$ref": "#/$defs/RecipeIdentityRef"
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
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
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
        "revision": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
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
        "source_collection_retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
        },
        "source_collection_retirement_policy": {
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
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipeDefinition",
  "unit": "export"
}
```

</details>
