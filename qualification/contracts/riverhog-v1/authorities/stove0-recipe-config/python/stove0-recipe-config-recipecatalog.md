# stove0_recipe_config.RecipeCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipecatalog:094db441fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-adb0115e53"></a>
- <a id="s-4c0c7b5ccc"></a>`distribution`: `stove0-recipe-config`
- <a id="s-37ab8166e1"></a>`module`: `stove0_recipe_config`
- <a id="s-6966dfe106"></a>`name`: `RecipeCatalog`
- <a id="s-548641cb4f"></a>`unit`: `export`

### Declared structure

- <a id="s-98a9d46505"></a>`kind`: `"class"`
- <a id="s-c3264f76c1"></a>`signature`: `"\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""`

#### Validated model schema

<a id="s-c4b7d2ccd1"></a>

- <a id="s-a53e37c2e1"></a>`type`: `"object"`
- <a id="s-a64090acb5"></a>`additionalProperties`: `false`
- <a id="s-de40acdeec"></a>`required`: `["operations","recipes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a8aa70fd73"></a>`format` | no | type="string"; const="stove0-recipes/v1"; default="stove0-recipes/v1" |  |
| <a id="s-d4fb01d14a"></a>`operations` | yes | type="array"; items=([OperationContract](#s-28d95a0ec8)) |  |
| <a id="s-93672e5a8c"></a>`recipes` | yes | type="array"; items=([RecipeDefinition](#s-e7eda17e90)) |  |

##### Definitions

- [ArtifactAssociation](#s-9152ec7164)
- [ArtifactFactBinding](#s-d7be78725a)
- [ArtifactRule](#s-38adfedc0e)
- [FactPredicate](#s-7a777308f1)
- [InputArtifactContract](#s-f70eda4717)
- [JsonSchemaDocument](#s-2b08201808)
- [JsonValue](#s-b65ed278e1)
- [ObserverUse](#s-790b310f20)
- [OperationContract](#s-28d95a0ec8)
- [OperationProjection](#s-66f9c8fe90)
- [OutputArtifactContract](#s-bb6addf750)
- [RecipeCoordinationRoute](#s-7fffe998a8)
- [RecipeDefinition](#s-e7eda17e90)
- [RecipeJoin](#s-f90bdb9aff)
- [RecipeJoinMember](#s-b47918d08a)
- [RecipeRef](#s-3785ff221b)
- [RecipeRoute](#s-bd46a2e35f)
- [SemanticValidationProfile](#s-b1189f8468)

##### <a id="s-9152ec7164"></a>definition `ArtifactAssociation`

- <a id="s-ad9da5a6ee"></a>`type`: `"object"`
- <a id="s-8040f0e04a"></a>`additionalProperties`: `false`
- <a id="s-e8cdd2d3d7"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1999293fb6"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-524670f1b6"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-8d9f9204a1"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-d7be78725a"></a>definition `ArtifactFactBinding`

- <a id="s-159c015c49"></a>`type`: `"object"`
- <a id="s-939bf061a1"></a>`additionalProperties`: `false`
- <a id="s-f56f1d7ad7"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4ba12602b"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-9c36b734c9"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-38adfedc0e"></a>definition `ArtifactRule`

- <a id="s-d50864bfcb"></a>`type`: `"object"`
- <a id="s-c0ee655284"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0085db5713"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-7394815699"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-b8673a9897"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-7a777308f1"></a>definition `FactPredicate`

- <a id="s-e7d41b8c32"></a>`type`: `"object"`
- <a id="s-fcf4b746de"></a>`additionalProperties`: `false`
- <a id="s-5b49e6d573"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c71484d2a8"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-d7be78725a)); (type="null")]; default=null |  |
| <a id="s-3e1984c3a0"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-a89a3f10f7"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-aa9096a0fd"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-cab8b2e515"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-a4e7d8b6a8"></a>`value` | no | [JsonValue](#s-b65ed278e1); default=null |  |

##### <a id="s-f70eda4717"></a>definition `InputArtifactContract`

- <a id="s-ada836e221"></a>`type`: `"object"`
- <a id="s-ceb81b2fe6"></a>`additionalProperties`: `false`
- <a id="s-9ffdeba195"></a>`required`: `["role"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-71c4831c2c"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null |  |
| <a id="s-3b849458fb"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-d30baad014"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-c0293264d6"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-2b08201808"></a>definition `JsonSchemaDocument`

- <a id="s-8f92e4572e"></a>`type`: `"object"`
- <a id="s-4b6a189132"></a>`additionalProperties`: `false`
- <a id="s-0ccb466dc5"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9bcf520e07"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-17da5cf85c"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-b487bb0ce8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b7b643a8c6"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-8a013a8dad"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b65ed278e1"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-790b310f20"></a>definition `ObserverUse`

- <a id="s-e7747dc047"></a>`type`: `"object"`
- <a id="s-242575d5a8"></a>`additionalProperties`: `false`
- <a id="s-7a4b163e27"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-735d326501"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38adfedc0e)) |  |
| <a id="s-866766bdef"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-409ab2b4bb"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c8478e9a97"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-edaa6248c4"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-fc9976ba9a"></a>`registration_id` | yes | type="string" |  |
| <a id="s-851eecd6d5"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-af47f04c34"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-28d95a0ec8"></a>definition `OperationContract`

- <a id="s-ac083e3287"></a>`type`: `"object"`
- <a id="s-65970fe5f7"></a>`additionalProperties`: `false`
- <a id="s-1c668db17f"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e0b418fe06"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-14e6b85bf2"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaDocument](#s-2b08201808)); (type="null")]; default=null |  |
| <a id="s-3f6107ca8b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fef90d3884"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-f70eda4717)); minItems=1 |  |
| <a id="s-4f48f5a5c8"></a>`intent_schema` | yes | [JsonSchemaDocument](#s-2b08201808) |  |
| <a id="s-79a740961d"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-b1189f8468) |  |
| <a id="s-a72979ea68"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-bb6addf750)) |  |
| <a id="s-8bb285899b"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-82a0f9f591"></a>`source_retirement_permitted` | no | type="boolean"; default=false |  |

##### <a id="s-66f9c8fe90"></a>definition `OperationProjection`

- <a id="s-9616091fc2"></a>`type`: `"object"`
- <a id="s-72168b9c1b"></a>`additionalProperties`: `false`
- <a id="s-cf3a3c0be4"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2bf4c176dd"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-5e86ca6f7f"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-59cbaeb5d8"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-685f2d7108"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-bb6addf750"></a>definition `OutputArtifactContract`

- <a id="s-d930881146"></a>`type`: `"object"`
- <a id="s-a7fc9b4a79"></a>`additionalProperties`: `false`
- <a id="s-5a03b30b59"></a>`required`: `["role","derived_from_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6cc0b3514"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-051a8d2e97"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-b7d8c4c685"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-62d40631a5"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-7fffe998a8"></a>definition `RecipeCoordinationRoute`

- <a id="s-cfb0b8a4d3"></a>`type`: `"object"`
- <a id="s-4b6e32a28b"></a>`additionalProperties`: `false`
- <a id="s-dd573c4544"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b4d729b033"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38adfedc0e)) |  |
| <a id="s-2c1d43910f"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-439f7c476e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e556ca450a"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-01b943ff0c"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-b13ceab449"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-0653c83bb4"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-66f9c8fe90)) |  |
| <a id="s-e3450d9114"></a>`recipe` | yes | [RecipeRef](#s-3785ff221b) |  |
| <a id="s-3620f4c4a7"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-7a777308f1)) |  |

##### <a id="s-e7eda17e90"></a>definition `RecipeDefinition`

- <a id="s-fcd345f8a6"></a>`type`: `"object"`
- <a id="s-cf5e646c74"></a>`additionalProperties`: `false`
- <a id="s-954c91de54"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-34e5d66873"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-0d4e98ed7d"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-9152ec7164)) |  |
| <a id="s-282fc71835"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-da56f25369"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0a47660473"></a>`join` | no | anyOf=[([RecipeJoin](#s-f90bdb9aff)); (type="null")]; default=null |  |
| <a id="s-4e0166b18f"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-790b310f20)) |  |
| <a id="s-8f5622e54f"></a>`retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-2fb56a366e"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-071ddd7df2"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-bd46a2e35f)); ([RecipeCoordinationRoute](#s-7fffe998a8))]); minItems=1 |  |
| <a id="s-ac9e8ca0f4"></a>`source_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-42be8ae529"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### <a id="s-f90bdb9aff"></a>definition `RecipeJoin`

- <a id="s-0b16b4fcf1"></a>`type`: `"object"`
- <a id="s-2bdd2fee2d"></a>`additionalProperties`: `false`
- <a id="s-08de048821"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9621b2009c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2803363728"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-dd4c3d4be6"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-ef0bf5dd6c"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-b47918d08a)); minItems=2 |  |
| <a id="s-7cf73bde2c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-aa1f3b58b5"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-66f9c8fe90)) |  |
| <a id="s-426c7316a6"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-9ec4cac1c2"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-b47918d08a"></a>definition `RecipeJoinMember`

- <a id="s-6904874aa4"></a>`type`: `"object"`
- <a id="s-44682fd7af"></a>`additionalProperties`: `false`
- <a id="s-a7397d6ad7"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b0bad1bcd"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-d96640dede"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-3785ff221b"></a>definition `RecipeRef`

- <a id="s-c7f5a51711"></a>`type`: `"object"`
- <a id="s-c57156a63d"></a>`additionalProperties`: `false`
- <a id="s-1cad6d83b8"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9dc99e64c5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-143ee7b9cd"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-a0044ca2eb"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-bd46a2e35f"></a>definition `RecipeRoute`

- <a id="s-d158a27bab"></a>`type`: `"object"`
- <a id="s-0afaeda5c6"></a>`additionalProperties`: `false`
- <a id="s-baac23310f"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0503587934"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38adfedc0e)) |  |
| <a id="s-7d77991627"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-a7ca68678e"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-82cc85f6ba"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-e163024952"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-e7c9bde254"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-faf506926d"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-1eaef3ef15"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-69eadf81f2"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-66f9c8fe90)) |  |
| <a id="s-905018cb52"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b65ed278e1)) |  |
| <a id="s-e32133db7a"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-163847e85a"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-7a777308f1)) |  |

##### <a id="s-b1189f8468"></a>definition `SemanticValidationProfile`

- <a id="s-562557203a"></a>`type`: `"object"`
- <a id="s-ef1d60db72"></a>`additionalProperties`: `false`
- <a id="s-005593e649"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4c7d00f71e"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-3777c6603a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7fc4ddd23d"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-928d973c0a"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [load](stove0-recipe-config-recipecatalog-load.md)
- [operation](stove0-recipe-config-recipecatalog-operation.md)
- [recipe](stove0-recipe-config-recipecatalog-recipe.md)
- [sha256](stove0-recipe-config-recipecatalog-sha256.md)
- [valid_catalog](stove0-recipe-config-recipecatalog-valid-catalog.md)
- [validation_document](stove0-recipe-config-recipecatalog-validation-document.md)

## Governing policies

- <a id="pa-f209886be8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — [reference/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeCatalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9c2e67feab967edb4ab19e4a27990b9351d7078c5e87158464c0406a8ca5a35 -->

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
        "InputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "allowed_dispositions": {
              "anyOf": [
                {
                  "items": {
                    "enum": [
                      "transformed",
                      "preserved",
                      "omitted",
                      "rejected"
                    ],
                    "type": "string"
                  },
                  "type": "array"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role"
          ],
          "type": "object"
        },
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
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
        "OperationContract": {
          "additionalProperties": false,
          "properties": {
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "effect_receipt_schema": {
              "anyOf": [
                {
                  "$ref": "#/$defs/JsonSchemaDocument"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "inputs": {
              "items": {
                "$ref": "#/$defs/InputArtifactContract"
              },
              "minItems": 1,
              "type": "array"
            },
            "intent_schema": {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            "intent_semantics": {
              "$ref": "#/$defs/SemanticValidationProfile"
            },
            "outputs": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OutputArtifactContract"
              },
              "type": "array"
            },
            "result_kind": {
              "default": "collection",
              "enum": [
                "collection",
                "external-effect"
              ],
              "type": "string"
            },
            "source_retirement_permitted": {
              "default": false,
              "type": "boolean"
            }
          },
          "required": [
            "id",
            "intent_schema",
            "intent_semantics",
            "inputs",
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
        "OutputArtifactContract": {
          "additionalProperties": false,
          "properties": {
            "derived_from_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "maximum": {
              "anyOf": [
                {
                  "minimum": 1,
                  "type": "integer"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "minimum": {
              "default": 1,
              "minimum": 0,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "derived_from_roles"
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
        "SemanticValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "conformance_vectors_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-recipes/v1",
          "default": "stove0-recipes/v1",
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/OperationContract"
          },
          "type": "array"
        },
        "recipes": {
          "items": {
            "$ref": "#/$defs/RecipeDefinition"
          },
          "type": "array"
        }
      },
      "required": [
        "operations",
        "recipes"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeCatalog",
  "unit": "export"
}
```

</details>
