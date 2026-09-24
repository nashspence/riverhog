# stove0-recipe-config:configuration:recipe-catalog configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-recipe-config:stove0-recipe-config-configuration-recipe-60aec4b96f:9fbd1881e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-e29fd5e2fc"></a>

- <a id="s-8918a16ca6"></a>`type`: `"object"`
- <a id="s-786ab3377e"></a>`additionalProperties`: `false`
- <a id="s-815fc08280"></a>`required`: `["operations","recipes"]`
- <a id="s-b23a81c067"></a>`title`: `"RecipeCatalog"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-31eae8ce6a"></a>`format` | no | type="string"; const="stove0-recipes/v1"; default="stove0-recipes/v1"; title="Format" |  |
| <a id="s-5ef17985fb"></a>`operations` | yes | type="array"; items=([OperationContract](#s-c65d8491da)); title="Operations" |  |
| <a id="s-ab7cd11b07"></a>`recipes` | yes | type="array"; items=([RecipeDefinition](#s-791feda911)); title="Recipes" |  |

### Definitions

- [ArtifactAssociation](#s-f8059f2549)
- [ArtifactFactBinding](#s-170e951eb4)
- [ArtifactRule](#s-6404b53e68)
- [FactPredicate](#s-0ec4242600)
- [InputArtifactContract](#s-c1f484a7f4)
- [JsonSchemaValidationProfile](#s-a6bf76e71f)
- [JsonValue](#s-a5f92f9e8f)
- [ObserverUse](#s-15f0f78bba)
- [OperationContract](#s-c65d8491da)
- [OperationProjection](#s-5422541e29)
- [OutputArtifactContract](#s-917836a5fe)
- [RecipeCoordinationRoute](#s-1ba6e7737f)
- [RecipeDefinition](#s-791feda911)
- [RecipeIdentityRef](#s-aeca9dca26)
- [RecipeJoin](#s-788d8a9692)
- [RecipeJoinMember](#s-562cd4bcc5)
- [RecipeRoute](#s-c9d6c9febb)
- [SemanticValidationProfile](#s-8c31943090)

### <a id="s-f8059f2549"></a>definition `ArtifactAssociation`

- <a id="s-c2ebfbc5c0"></a>`type`: `"object"`
- <a id="s-8214699fdd"></a>`additionalProperties`: `false`
- <a id="s-ce2b5cef43"></a>`description`: `"Associate classified artifacts without assigning device meaning to Stove0."`
- <a id="s-5b11399796"></a>`required`: `["primary_role","associated_roles"]`
- <a id="s-256dd4cab6"></a>`title`: `"ArtifactAssociation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eaa6d3429a"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Associated Roles" |  |
| <a id="s-b91b9334a8"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem"; title="Path Identity" |  |
| <a id="s-f416113227"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Role" |  |

### <a id="s-170e951eb4"></a>definition `ArtifactFactBinding`

- <a id="s-3b8b198a3e"></a>`type`: `"object"`
- <a id="s-1a024bf992"></a>`additionalProperties`: `false`
- <a id="s-616632ab0a"></a>`description`: `"Locate subject-keyed records inside one observer's declared facts schema."`
- <a id="s-aa9b73b4d7"></a>`required`: `["records_pointer"]`
- <a id="s-0de4f524fe"></a>`title`: `"ArtifactFactBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e96c9129e2"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Artifact Id Pointer" |  |
| <a id="s-351547b26e"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Records Pointer" |  |

### <a id="s-6404b53e68"></a>definition `ArtifactRule`

- <a id="s-995c25f891"></a>`type`: `"object"`
- <a id="s-dd4307c5e5"></a>`additionalProperties`: `false`
- <a id="s-5d1b4e26e7"></a>`description`: `"Classify one path; first matching rule wins."`
- <a id="s-08999c5b6f"></a>`title`: `"ArtifactRule"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-86f89c610b"></a>`glob` | no | type="string"; default="*"; title="Glob" |  |
| <a id="s-6b32ebf9bf"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-bd26e24344"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-0ec4242600"></a>definition `FactPredicate`

- <a id="s-649ffef398"></a>`type`: `"object"`
- <a id="s-ffc7576b40"></a>`additionalProperties`: `false`
- <a id="s-2e7fce569e"></a>`required`: `["observation_contract_id","pointer"]`
- <a id="s-c3691bb623"></a>`title`: `"FactPredicate"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e583816cbe"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-170e951eb4)); (type="null")]; default=null |  |
| <a id="s-e59e634c3e"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Artifact Roles" |  |
| <a id="s-b2b092b558"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observation Contract Id" |  |
| <a id="s-80cb39e323"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals"; title="Operator" |  |
| <a id="s-65cf2b0680"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Pointer" |  |
| <a id="s-c92ae38de1"></a>`value` | no | [JsonValue](#s-a5f92f9e8f); default=null |  |

### <a id="s-c1f484a7f4"></a>definition `InputArtifactContract`

- <a id="s-4933dc7f85"></a>`type`: `"object"`
- <a id="s-75b4f7659b"></a>`additionalProperties`: `false`
- <a id="s-a0c4a75a82"></a>`required`: `["role"]`
- <a id="s-5e9463227f"></a>`title`: `"InputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c2391c44b4"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null; title="Allowed Dispositions" |  |
| <a id="s-67726c67a0"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-94e09ee3ba"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-b808167fd6"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-a6bf76e71f"></a>definition `JsonSchemaValidationProfile`

- <a id="s-e1b836041b"></a>`type`: `"object"`
- <a id="s-7b35310084"></a>`additionalProperties`: `false`
- <a id="s-36fb08245a"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-1c2c93292b"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aaeba4a945"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-ab1d8cb473"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-b7483478f9"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-330832666d"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-7b2232f1a2"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Schema" |  |

### <a id="s-a5f92f9e8f"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-15f0f78bba"></a>definition `ObserverUse`

- <a id="s-28a7c4b811"></a>`type`: `"object"`
- <a id="s-08b9bca451"></a>`additionalProperties`: `false`
- <a id="s-3e73319d0c"></a>`required`: `["registration_id","contract_id","contract_sha256"]`
- <a id="s-e5ca3defe3"></a>`title`: `"ObserverUse"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d24cae629"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6404b53e68)); title="Artifact Rules" |  |
| <a id="s-3abd053a2d"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Contract Id" |  |
| <a id="s-b49223eddd"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-fe439b004e"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-c9abdba6cc"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Options" |  |
| <a id="s-3876883be9"></a>`registration_id` | yes | type="string"; title="Registration Id" |  |
| <a id="s-99924efcce"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-e18a52cac6"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |

### <a id="s-c65d8491da"></a>definition `OperationContract`

- <a id="s-f36c14170b"></a>`type`: `"object"`
- <a id="s-fa2ca4339d"></a>`additionalProperties`: `false`
- <a id="s-880efc24b9"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`
- <a id="s-9d3422dead"></a>`title`: `"OperationContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8e19384d9"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-51d3cc22c9"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-a6bf76e71f)); (type="null")]; default=null |  |
| <a id="s-0a288e1f78"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-7504707457"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-c1f484a7f4)); minItems=1; title="Inputs" |  |
| <a id="s-9d77312269"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-a6bf76e71f) |  |
| <a id="s-02f71f2d20"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-8c31943090) |  |
| <a id="s-13d6cea42d"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-917836a5fe)); title="Outputs" |  |
| <a id="s-b3c7e1132c"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-1772d8849c"></a>`source_collection_retirement_permitted` | no | type="boolean"; default=false; title="Source Collection Retirement Permitted" |  |

### <a id="s-5422541e29"></a>definition `OperationProjection`

- <a id="s-d4b51ff907"></a>`type`: `"object"`
- <a id="s-c594c006f6"></a>`additionalProperties`: `false`
- <a id="s-fbf204bdeb"></a>`description`: `"One declarative JSON-pointer copy into an operation request."`
- <a id="s-e87637d035"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`
- <a id="s-dfabe6aaa0"></a>`title`: `"OperationProjection"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-549436221b"></a>`destination` | yes | type="string"; enum=["intent","target-options"]; title="Destination" |  |
| <a id="s-9f646b99b9"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Destination Pointer" |  |
| <a id="s-b9e4348644"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"]; title="Source" |  |
| <a id="s-ea2c2cdb7d"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Source Pointer" |  |

### <a id="s-917836a5fe"></a>definition `OutputArtifactContract`

- <a id="s-bfbd57e6dd"></a>`type`: `"object"`
- <a id="s-6da1eb02dc"></a>`additionalProperties`: `false`
- <a id="s-4b9292538c"></a>`required`: `["role","derived_from_roles"]`
- <a id="s-44ecbd58ca"></a>`title`: `"OutputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60dfafbb69"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Derived From Roles" |  |
| <a id="s-05bb0356f3"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-f59a6ea05b"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-493c19ce32"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-1ba6e7737f"></a>definition `RecipeCoordinationRoute`

- <a id="s-799dd6606b"></a>`type`: `"object"`
- <a id="s-9415426b67"></a>`additionalProperties`: `false`
- <a id="s-368177c3f6"></a>`description`: `"One exact subrecipe selected as a branch-bound coordinator."`
- <a id="s-3f820d641f"></a>`required`: `["id","recipe"]`
- <a id="s-a31915d482"></a>`title`: `"RecipeCoordinationRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-73a10157d5"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6404b53e68)); title="Artifact Rules" |  |
| <a id="s-d28760d4f8"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-9565d64c36"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-2a305fe98b"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Intent" |  |
| <a id="s-402380da69"></a>`kind` | no | type="string"; const="coordination"; default="coordination"; title="Kind" |  |
| <a id="s-ae969d30d2"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-7186ce6f00"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5422541e29)); title="Projections" |  |
| <a id="s-0c990c27dd"></a>`recipe` | yes | [RecipeIdentityRef](#s-aeca9dca26) |  |
| <a id="s-3be045bd6f"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-0ec4242600)); title="When" |  |

### <a id="s-791feda911"></a>definition `RecipeDefinition`

- <a id="s-b0050fef22"></a>`type`: `"object"`
- <a id="s-b0b9fd662c"></a>`additionalProperties`: `false`
- <a id="s-33bf68a109"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`
- <a id="s-6b908442f0"></a>`title`: `"RecipeDefinition"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-586a4ddc64"></a>`allow_derived_inputs` | no | type="boolean"; default=false; title="Allow Derived Inputs" |  |
| <a id="s-10c77901d6"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-f8059f2549)); title="Artifact Associations" |  |
| <a id="s-f3a0761449"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection"; title="Event Input Closure" |  |
| <a id="s-8c4eebf6e1"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-90d281d507"></a>`join` | no | anyOf=[([RecipeJoin](#s-788d8a9692)); (type="null")]; default=null |  |
| <a id="s-a8b913aeb1"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-15f0f78bba)); title="Observers" |  |
| <a id="s-6210f1cfe6"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-8198a7011b"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-c9d6c9febb)); ([RecipeCoordinationRoute](#s-1ba6e7737f))]); minItems=1; title="Routes" |  |
| <a id="s-d10c16c90b"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-eaea3e8e56"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-a00ccda986"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"]; title="Unmatched Artifact Disposition" |  |

### <a id="s-aeca9dca26"></a>definition `RecipeIdentityRef`

- <a id="s-c297926d04"></a>`type`: `"object"`
- <a id="s-07e126a0e8"></a>`additionalProperties`: `false`
- <a id="s-36a2a2211d"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-1db421badf"></a>`required`: `["id","revision","sha256"]`
- <a id="s-ee7e8bbe88"></a>`title`: `"RecipeIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d70d18e4ff"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-e84f83e344"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-7c204efddc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-788d8a9692"></a>definition `RecipeJoin`

- <a id="s-d2cb9b329a"></a>`type`: `"object"`
- <a id="s-107c8f6c28"></a>`additionalProperties`: `false`
- <a id="s-344f14b320"></a>`required`: `["id","members","operation_id","target_registration_id"]`
- <a id="s-682769c46b"></a>`title`: `"RecipeJoin"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9ac56de3a1"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-b9c2f1ac85"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-c682992aa6"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Intent" |  |
| <a id="s-05531ef714"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-562cd4bcc5)); minItems=2; title="Members" |  |
| <a id="s-c425b94632"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-8e44f54012"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5422541e29)); title="Projections" |  |
| <a id="s-ff5cb6c5fc"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Target Options" |  |
| <a id="s-7073a43b26"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |

### <a id="s-562cd4bcc5"></a>definition `RecipeJoinMember`

- <a id="s-18ebab8fbc"></a>`type`: `"object"`
- <a id="s-67c23a6e3f"></a>`additionalProperties`: `false`
- <a id="s-a9f3780417"></a>`required`: `["branch_id","output_roles"]`
- <a id="s-bad6f4b052"></a>`title`: `"RecipeJoinMember"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bebb1d93d7"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-6a4326eecd"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Output Roles" |  |

### <a id="s-c9d6c9febb"></a>definition `RecipeRoute`

- <a id="s-2c8429c285"></a>`type`: `"object"`
- <a id="s-ac284da5ca"></a>`additionalProperties`: `false`
- <a id="s-448e59e566"></a>`description`: `"One ordinary target/effect leaf selected by a recipe."`
- <a id="s-b79a97e888"></a>`required`: `["id","operation_id","target_registration_id"]`
- <a id="s-e15275f372"></a>`title`: `"RecipeRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c07c17bb3a"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-6404b53e68)); title="Artifact Rules" |  |
| <a id="s-4dd7c0e382"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-d89b9127fa"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-a323ffbb3a"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-11242044dd"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Intent" |  |
| <a id="s-c30f5baccd"></a>`kind` | no | type="string"; const="operation"; default="operation"; title="Kind" |  |
| <a id="s-263b4f7e13"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-2722a1d6e0"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-f81f6f5e02"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-5422541e29)); title="Projections" |  |
| <a id="s-d8e1c231d2"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-a5f92f9e8f)); title="Target Options" |  |
| <a id="s-4736292fba"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |
| <a id="s-532d32437b"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-0ec4242600)); title="When" |  |

### <a id="s-8c31943090"></a>definition `SemanticValidationProfile`

- <a id="s-9519b8b2e3"></a>`type`: `"object"`
- <a id="s-8e19047cb2"></a>`additionalProperties`: `false`
- <a id="s-f3d4c4d7dc"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-6c2ffacf8d"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c11f2e5e6c"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-a61b427bde"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-ad6f2a2d63"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-5868b02446"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-recipe-config:configuration:recipe-catalog"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition ArtifactAssociation · field associated_roles](#s-eaa6d3429a) | `cardinality · items · operational_policy` | shared above |
| [definition FactPredicate · field artifact_roles](#s-e59e634c3e) | `cardinality · items · operational_policy` | shared above |
| <a id="s-0c224251e1"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-c2391c44b4) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-7b2232f1a2) | `cardinality · entries · operational_policy` | shared above |
| [definition ObserverUse · field artifact_rules](#s-9d24cae629) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverUse · field options](#s-c9abdba6cc) | `cardinality · entries · operational_policy` | shared above |
| [definition OperationContract · field inputs](#s-7504707457) | `cardinality · items · operational_policy` | shared above |
| [definition OperationContract · field outputs](#s-13d6cea42d) | `cardinality · items · operational_policy` | shared above |
| [definition OutputArtifactContract · field derived_from_roles](#s-60dfafbb69) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field artifact_rules](#s-73a10157d5) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field associated_roles](#s-d28760d4f8) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field intent](#s-2a305fe98b) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field projections](#s-7186ce6f00) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field when](#s-3be045bd6f) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field artifact_associations](#s-10c77901d6) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field observers](#s-a8b913aeb1) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field routes](#s-8198a7011b) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field intent](#s-c682992aa6) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoin · field members](#s-05531ef714) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field projections](#s-8e44f54012) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field target_options](#s-ff5cb6c5fc) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoinMember · field output_roles](#s-6a4326eecd) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field artifact_rules](#s-c07c17bb3a) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field associated_roles](#s-4dd7c0e382) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field intent](#s-11242044dd) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field projections](#s-f81f6f5e02) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field target_options](#s-d8e1c231d2) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field when](#s-532d32437b) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticValidationProfile · field rules](#s-5868b02446) | `cardinality · items · operational_policy` | shared above |
| [field operations](#s-5ef17985fb) | `cardinality · items · operational_policy` | shared above |
| [field recipes](#s-ab7cd11b07) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-330832666d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field contract_sha256](#s-b49223eddd) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field maximum_result_bytes](#s-fe439b004e) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ObserverUse · field timeout_seconds](#s-e18a52cac6) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition OperationContract · field contract_sha256](#s-e8e19384d9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition RecipeIdentityRef · field sha256](#s-7c204efddc) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-78cbcbe687"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-c11f2e5e6c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SemanticValidationProfile · field profile_sha256](#s-ad6f2a2d63) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d0c3694e90"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-bdcbfcfe05"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-e9e4492790"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-recipe-config:configuration:recipe-catalog](../../../evidence/sources/authorities.md#src-28686050a7) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/models.py::RecipeCatalog](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/models.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/stove0-recipe-config:configuration:recipe-catalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b840ab4ce734e65a87bad9160a9fd0a768313b68e8c9154f6cf35539c338574d -->

```json
{
  "$defs": {
    "ArtifactAssociation": {
      "additionalProperties": false,
      "description": "Associate classified artifacts without assigning device meaning to Stove0.",
      "properties": {
        "associated_roles": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Associated Roles",
          "type": "array"
        },
        "path_identity": {
          "const": "same-parent-stem",
          "default": "same-parent-stem",
          "title": "Path Identity",
          "type": "string"
        },
        "primary_role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Primary Role",
          "type": "string"
        }
      },
      "required": [
        "primary_role",
        "associated_roles"
      ],
      "title": "ArtifactAssociation",
      "type": "object"
    },
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
    "ArtifactRule": {
      "additionalProperties": false,
      "description": "Classify one path; first matching rule wins.",
      "properties": {
        "glob": {
          "default": "*",
          "title": "Glob",
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
          "default": null,
          "title": "Media Type"
        },
        "role": {
          "default": "stove0.source/v1",
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "title": "ArtifactRule",
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
          "default": null,
          "title": "Allowed Dispositions"
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
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role"
      ],
      "title": "InputArtifactContract",
      "type": "object"
    },
    "JsonSchemaValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        }
      },
      "required": [
        "id",
        "profile_sha256",
        "schema"
      ],
      "title": "JsonSchemaValidationProfile",
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
          "title": "Artifact Rules",
          "type": "array"
        },
        "contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "registration_id": {
          "title": "Registration Id",
          "type": "string"
        },
        "retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Retrieval Policy",
          "type": "string"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        }
      },
      "required": [
        "registration_id",
        "contract_id",
        "contract_sha256"
      ],
      "title": "ObserverUse",
      "type": "object"
    },
    "OperationContract": {
      "additionalProperties": false,
      "properties": {
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
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
          "title": "Id",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/InputArtifactContract"
          },
          "minItems": 1,
          "title": "Inputs",
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
          "title": "Outputs",
          "type": "array"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        },
        "source_collection_retirement_permitted": {
          "default": false,
          "title": "Source Collection Retirement Permitted",
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
      "title": "OperationContract",
      "type": "object"
    },
    "OperationProjection": {
      "additionalProperties": false,
      "description": "One declarative JSON-pointer copy into an operation request.",
      "properties": {
        "destination": {
          "enum": [
            "intent",
            "target-options"
          ],
          "title": "Destination",
          "type": "string"
        },
        "destination_pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Destination Pointer",
          "type": "string"
        },
        "source": {
          "enum": [
            "work-effective-intent",
            "work-evaluation"
          ],
          "title": "Source",
          "type": "string"
        },
        "source_pointer": {
          "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
          "title": "Source Pointer",
          "type": "string"
        }
      },
      "required": [
        "source",
        "source_pointer",
        "destination",
        "destination_pointer"
      ],
      "title": "OperationProjection",
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
          "title": "Derived From Roles",
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
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role",
        "derived_from_roles"
      ],
      "title": "OutputArtifactContract",
      "type": "object"
    },
    "RecipeCoordinationRoute": {
      "additionalProperties": false,
      "description": "One exact subrecipe selected as a branch-bound coordinator.",
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
          "title": "Artifact Rules",
          "type": "array"
        },
        "associated_roles": {
          "default": [],
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "title": "Associated Roles",
          "type": "array"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "kind": {
          "const": "coordination",
          "default": "coordination",
          "title": "Kind",
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
          "default": null,
          "title": "Primary Role"
        },
        "projections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OperationProjection"
          },
          "title": "Projections",
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
          "title": "When",
          "type": "array"
        }
      },
      "required": [
        "id",
        "recipe"
      ],
      "title": "RecipeCoordinationRoute",
      "type": "object"
    },
    "RecipeDefinition": {
      "additionalProperties": false,
      "properties": {
        "allow_derived_inputs": {
          "default": false,
          "title": "Allow Derived Inputs",
          "type": "boolean"
        },
        "artifact_associations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactAssociation"
          },
          "title": "Artifact Associations",
          "type": "array"
        },
        "event_input_closure": {
          "const": "single-finalized-collection",
          "default": "single-finalized-collection",
          "title": "Event Input Closure",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
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
          "title": "Observers",
          "type": "array"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
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
          "title": "Routes",
          "type": "array"
        },
        "source_collection_retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "title": "Source Collection Retirement Grace Seconds",
          "type": "integer"
        },
        "source_collection_retirement_policy": {
          "default": "retain",
          "description": "Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks.",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "title": "Source Collection Retirement Policy",
          "type": "string"
        },
        "unmatched_artifact_disposition": {
          "enum": [
            "retain-in-source",
            "reject-work"
          ],
          "title": "Unmatched Artifact Disposition",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "routes",
        "unmatched_artifact_disposition"
      ],
      "title": "RecipeDefinition",
      "type": "object"
    },
    "RecipeIdentityRef": {
      "additionalProperties": false,
      "description": "Embedded Stove0 reference to the Riverhog recipe identity.",
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "sha256"
      ],
      "title": "RecipeIdentityRef",
      "type": "object"
    },
    "RecipeJoin": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "input_retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Input Retrieval Policy",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/RecipeJoinMember"
          },
          "minItems": 2,
          "title": "Members",
          "type": "array"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "projections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OperationProjection"
          },
          "title": "Projections",
          "type": "array"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        },
        "target_registration_id": {
          "title": "Target Registration Id",
          "type": "string"
        }
      },
      "required": [
        "id",
        "members",
        "operation_id",
        "target_registration_id"
      ],
      "title": "RecipeJoin",
      "type": "object"
    },
    "RecipeJoinMember": {
      "additionalProperties": false,
      "properties": {
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
          "type": "string"
        },
        "output_roles": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Output Roles",
          "type": "array"
        }
      },
      "required": [
        "branch_id",
        "output_roles"
      ],
      "title": "RecipeJoinMember",
      "type": "object"
    },
    "RecipeRoute": {
      "additionalProperties": false,
      "description": "One ordinary target/effect leaf selected by a recipe.",
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
          "title": "Artifact Rules",
          "type": "array"
        },
        "associated_roles": {
          "default": [],
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "title": "Associated Roles",
          "type": "array"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "input_retrieval_policy": {
          "default": "available-only",
          "enum": [
            "available-only",
            "allow"
          ],
          "title": "Input Retrieval Policy",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "kind": {
          "const": "operation",
          "default": "operation",
          "title": "Kind",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
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
          "default": null,
          "title": "Primary Role"
        },
        "projections": {
          "default": [],
          "items": {
            "$ref": "#/$defs/OperationProjection"
          },
          "title": "Projections",
          "type": "array"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        },
        "target_registration_id": {
          "title": "Target Registration Id",
          "type": "string"
        },
        "when": {
          "default": [],
          "items": {
            "$ref": "#/$defs/FactPredicate"
          },
          "title": "When",
          "type": "array"
        }
      },
      "required": [
        "id",
        "operation_id",
        "target_registration_id"
      ],
      "title": "RecipeRoute",
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
          "default": null,
          "title": "Conformance Vectors Sha256"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rules": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Rules",
          "type": "array"
        }
      },
      "required": [
        "id",
        "rules",
        "profile_sha256"
      ],
      "title": "SemanticValidationProfile",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-recipes/v1",
      "default": "stove0-recipes/v1",
      "title": "Format",
      "type": "string"
    },
    "operations": {
      "items": {
        "$ref": "#/$defs/OperationContract"
      },
      "title": "Operations",
      "type": "array"
    },
    "recipes": {
      "items": {
        "$ref": "#/$defs/RecipeDefinition"
      },
      "title": "Recipes",
      "type": "array"
    }
  },
  "required": [
    "operations",
    "recipes"
  ],
  "title": "RecipeCatalog",
  "type": "object"
}
```

</details>
