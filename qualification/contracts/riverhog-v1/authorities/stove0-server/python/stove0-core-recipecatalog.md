# stove0_core.RecipeCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog:212333cc41 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac1bf7f772"></a>
- <a id="s-9a95dda91f"></a>`distribution`: `stove0-server`
- <a id="s-e81443c24c"></a>`module`: `stove0_core`
- <a id="s-71d5956b6b"></a>`name`: `RecipeCatalog`
- <a id="s-1b9a890ee0"></a>`unit`: `export`

### Declared structure

- <a id="s-dd34918488"></a>`kind`: `"class"`
- <a id="s-f47e6b3ead"></a>`signature`: `"\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""`

#### Validated model schema

<a id="s-7eab2c5d80"></a>

- <a id="s-b6f14aa630"></a>`type`: `"object"`
- <a id="s-1aa54382f6"></a>`additionalProperties`: `false`
- <a id="s-72495cd0d1"></a>`required`: `["operations","recipes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4c73cd4611"></a>`format` | no | type="string"; const="stove0-recipes/v1"; default="stove0-recipes/v1" |  |
| <a id="s-e8c4db3c48"></a>`operations` | yes | type="array"; items=([OperationContract](#s-42d3f33c91)) |  |
| <a id="s-d2dd1a7761"></a>`recipes` | yes | type="array"; items=([RecipeDefinition](#s-d7a456910c)) |  |

##### Definitions

- [ArtifactAssociation](#s-4ea94d6958)
- [ArtifactFactBinding](#s-f35ed8e977)
- [ArtifactRule](#s-321bcd0181)
- [FactPredicate](#s-faa5d4d6cc)
- [InputArtifactContract](#s-893e7a1885)
- [JsonSchemaValidationProfile](#s-aabf222999)
- [JsonValue](#s-655fdbe95a)
- [ObserverUse](#s-ead92c6441)
- [OperationContract](#s-42d3f33c91)
- [OperationProjection](#s-bd29d8e8f0)
- [OutputArtifactContract](#s-4ebe2c544b)
- [RecipeCoordinationRoute](#s-0e7e1d3689)
- [RecipeDefinition](#s-d7a456910c)
- [RecipeJoin](#s-ffedb1726b)
- [RecipeJoinMember](#s-33c0aa00c7)
- [RecipeRef](#s-968cd63522)
- [RecipeRoute](#s-e0a2848845)
- [SemanticValidationProfile](#s-467a54d056)

##### <a id="s-4ea94d6958"></a>definition `ArtifactAssociation`

- <a id="s-1c496a2b54"></a>`type`: `"object"`
- <a id="s-2a480b162d"></a>`additionalProperties`: `false`
- <a id="s-032b536606"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2a54a855b3"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-52d9e08419"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-f8820f7653"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-f35ed8e977"></a>definition `ArtifactFactBinding`

- <a id="s-05de6157c8"></a>`type`: `"object"`
- <a id="s-c22010a26a"></a>`additionalProperties`: `false`
- <a id="s-5f5208eeca"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df5f2151b9"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-1faa5ec7f0"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-321bcd0181"></a>definition `ArtifactRule`

- <a id="s-6bdc28415e"></a>`type`: `"object"`
- <a id="s-1bd1d66fb0"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b71e9f7554"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-5976115f87"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-6b9ee6c6d7"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-faa5d4d6cc"></a>definition `FactPredicate`

- <a id="s-cadb9b4f08"></a>`type`: `"object"`
- <a id="s-7d6bfaf7e7"></a>`additionalProperties`: `false`
- <a id="s-2aa9986485"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-38c4e17d27"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-f35ed8e977)); (type="null")]; default=null |  |
| <a id="s-dc2e7337db"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-afd2363b60"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-37e1788109"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-7ae3fe0408"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-6ddcfe2337"></a>`value` | no | [JsonValue](#s-655fdbe95a); default=null |  |

##### <a id="s-893e7a1885"></a>definition `InputArtifactContract`

- <a id="s-def7db480f"></a>`type`: `"object"`
- <a id="s-0f3292cbb5"></a>`additionalProperties`: `false`
- <a id="s-54a938d562"></a>`required`: `["role"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-feac620b6f"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null |  |
| <a id="s-57da827557"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-e0c3ac6573"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-80105dd786"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-aabf222999"></a>definition `JsonSchemaValidationProfile`

- <a id="s-c9625a9d00"></a>`type`: `"object"`
- <a id="s-bdae2d5387"></a>`additionalProperties`: `false`
- <a id="s-07b0979c13"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d715c7a4d9"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-8f3b1260fb"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-c070004279"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-74f92323c3"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4673823f15"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |

##### <a id="s-655fdbe95a"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-ead92c6441"></a>definition `ObserverUse`

- <a id="s-a73c7d537c"></a>`type`: `"object"`
- <a id="s-230d29f665"></a>`additionalProperties`: `false`
- <a id="s-5259367846"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2912a35e65"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-321bcd0181)) |  |
| <a id="s-c867e6e905"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7b85404302"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ef3cc259b3"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-5f6bc3b493"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-71012c5997"></a>`registration_id` | yes | type="string" |  |
| <a id="s-ad835fb649"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-3661a19a33"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-42d3f33c91"></a>definition `OperationContract`

- <a id="s-e28b4812bf"></a>`type`: `"object"`
- <a id="s-5d8d0790df"></a>`additionalProperties`: `false`
- <a id="s-f99cfd4c9e"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65cbffa857"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a75ce4856f"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-aabf222999)); (type="null")]; default=null |  |
| <a id="s-37654f4f87"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fe1578028d"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-893e7a1885)); minItems=1 |  |
| <a id="s-fc7522a438"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-aabf222999) |  |
| <a id="s-249f692d35"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-467a54d056) |  |
| <a id="s-fde0449546"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-4ebe2c544b)) |  |
| <a id="s-70a075cf49"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |
| <a id="s-bd0d23046b"></a>`source_collection_retirement_permitted` | no | type="boolean"; default=false |  |

##### <a id="s-bd29d8e8f0"></a>definition `OperationProjection`

- <a id="s-442bc55b70"></a>`type`: `"object"`
- <a id="s-6c4ae4b6d8"></a>`additionalProperties`: `false`
- <a id="s-10c99afdfd"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-616c4172f2"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-f1bd03dc96"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-8cd8e9d6f0"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-1c0d7d0e00"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-4ebe2c544b"></a>definition `OutputArtifactContract`

- <a id="s-e749847a59"></a>`type`: `"object"`
- <a id="s-ff795babbc"></a>`additionalProperties`: `false`
- <a id="s-539e5f68d8"></a>`required`: `["role","derived_from_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-20259edb5c"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-f6400a52fe"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-3f2b668258"></a>`minimum` | no | type="integer"; minimum=0; default=1 |  |
| <a id="s-e56480d27b"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-0e7e1d3689"></a>definition `RecipeCoordinationRoute`

- <a id="s-e739b635cd"></a>`type`: `"object"`
- <a id="s-8119382c97"></a>`additionalProperties`: `false`
- <a id="s-3f3ca6c3f4"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b279e9b936"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-321bcd0181)) |  |
| <a id="s-d36cc8ed49"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-f902f4af2c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-99fd8ad45e"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-25a8ca70d9"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-ba1aa0ba3d"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-60b49858f4"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-bd29d8e8f0)) |  |
| <a id="s-1e2cfb4802"></a>`recipe` | yes | [RecipeRef](#s-968cd63522) |  |
| <a id="s-88a0355e0e"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-faa5d4d6cc)) |  |

##### <a id="s-d7a456910c"></a>definition `RecipeDefinition`

- <a id="s-e5b2dafcf6"></a>`type`: `"object"`
- <a id="s-7194194354"></a>`additionalProperties`: `false`
- <a id="s-5f300dd0d1"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33934f5939"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-65623ccf07"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-4ea94d6958)) |  |
| <a id="s-7ae6d978c6"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-8b7c8a0bd6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-89f735570e"></a>`join` | no | anyOf=[([RecipeJoin](#s-ffedb1726b)); (type="null")]; default=null |  |
| <a id="s-dc666d1ddd"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-ead92c6441)) |  |
| <a id="s-b91532e55d"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-3e17379ac4"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-e0a2848845)); ([RecipeCoordinationRoute](#s-0e7e1d3689))]); minItems=1 |  |
| <a id="s-75e0010448"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-1026cdea7a"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-7f714f0108"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### <a id="s-ffedb1726b"></a>definition `RecipeJoin`

- <a id="s-163f4cc2af"></a>`type`: `"object"`
- <a id="s-f3a27c108f"></a>`additionalProperties`: `false`
- <a id="s-bf53875a72"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-28a9ff7668"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a153b2f717"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-08b03466f3"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-4fd77e151a"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-33c0aa00c7)); minItems=2 |  |
| <a id="s-b69fc30ff8"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-010de498d8"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-bd29d8e8f0)) |  |
| <a id="s-3d7fc994fa"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-f152e3be83"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-33c0aa00c7"></a>definition `RecipeJoinMember`

- <a id="s-f60fa18721"></a>`type`: `"object"`
- <a id="s-f36cb8cc0e"></a>`additionalProperties`: `false`
- <a id="s-44ce7d3169"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f9cae0ecd"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f68f27a2a4"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-968cd63522"></a>definition `RecipeRef`

- <a id="s-0807f870e4"></a>`type`: `"object"`
- <a id="s-ef7b39e4e5"></a>`additionalProperties`: `false`
- <a id="s-52cea32348"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40f52e1b3d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-49fe8fdcd8"></a>`revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-74672470ac"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-e0a2848845"></a>definition `RecipeRoute`

- <a id="s-3d00f1299b"></a>`type`: `"object"`
- <a id="s-45acb7f120"></a>`additionalProperties`: `false`
- <a id="s-2886b1d246"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0c43da5ed4"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-321bcd0181)) |  |
| <a id="s-504400fb31"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-608b94d18a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-21089d313d"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-cdd0082ecb"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-f9899b24b7"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-4f73dc96ce"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-fbc54fb191"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-984029fbc3"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-bd29d8e8f0)) |  |
| <a id="s-f3684b5aa6"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-655fdbe95a)) |  |
| <a id="s-546a2e17d5"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-e4e093f3bc"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-faa5d4d6cc)) |  |

##### <a id="s-467a54d056"></a>definition `SemanticValidationProfile`

- <a id="s-493f352f77"></a>`type`: `"object"`
- <a id="s-8c770e8eb0"></a>`additionalProperties`: `false`
- <a id="s-c1d7de8e8f"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3874a76fdb"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a262f66ffa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e58dd09b39"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-54b05c56e9"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [load](stove0-core-recipecatalog-load.md)
- [operation](stove0-core-recipecatalog-operation.md)
- [recipe](stove0-core-recipecatalog-recipe.md)
- [sha256](stove0-core-recipecatalog-sha256.md)
- [valid_catalog](stove0-core-recipecatalog-valid-catalog.md)
- [validation_document](stove0-core-recipecatalog-validation-document.md)

## Governing policies

- <a id="pa-d040925c78"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2b76b5356c42ea24e61170a152a6bb5874adaf9d97370600fdd43ea56701401 -->

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
        "JsonSchemaValidationProfile": {
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
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
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
                  "$ref": "#/$defs/JsonSchemaValidationProfile"
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
              "$ref": "#/$defs/JsonSchemaValidationProfile"
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
            "source_collection_retirement_permitted": {
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
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipeCatalog",
  "unit": "export"
}
```

</details>
