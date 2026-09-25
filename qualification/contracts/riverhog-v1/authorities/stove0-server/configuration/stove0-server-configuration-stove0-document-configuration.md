# stove0-server:configuration:stove0-document configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-server:stove0-server-configuration-stove0-docume-54bc5c97ac:b33400d8ab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-04c35a9827"></a>

- <a id="s-ad4c8a81e8"></a>`type`: `"object"`
- <a id="s-6d928084d7"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json"`
- <a id="s-7e8ad6a049"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-6e39920e25"></a>`additionalProperties`: `false`
- <a id="s-0b4227247a"></a>`required`: `["database_url_file","riverhog_base_url","riverhog_token_file","recipes","declared_workspace_protection","browse_token_signing_key_file"]`
- <a id="s-0049299fa3"></a>`title`: `"Stove0Document"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-907dbb74b4"></a>`admissions` | no | [AdmissionCatalog](#s-fe81d7a141) |  |
| <a id="s-2ad2c72717"></a>`api_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Api Token File" |  |
| <a id="s-62ad81fb10"></a>`browse_token_lifetime_seconds` | no | type="integer"; minimum=1; default=86400; title="Browse Token Lifetime Seconds" |  |
| <a id="s-638501eaf4"></a>`browse_token_signing_key_file` | yes | type="string"; format="path"; title="Browse Token Signing Key File" |  |
| <a id="s-c5fe35b4df"></a>`capability_ttl_seconds` | no | type="integer"; minimum=30; default=900; title="Capability Ttl Seconds" |  |
| <a id="s-6bc26f0384"></a>`claim_lease_seconds` | no | type="integer"; minimum=30; default=1800; title="Claim Lease Seconds" |  |
| <a id="s-c608c4469c"></a>`database_url_file` | yes | type="string"; format="path"; title="Database Url File" |  |
| <a id="s-3774701a4f"></a>`declared_workspace_protection` | yes | type="string"; enum=["encrypted-at-rest","memory-backed"]; title="Declared Workspace Protection" |  |
| <a id="s-df3397f280"></a>`departure_targets` | no | type="object"; additionalProperties=([EndpointDocument](#s-cb60ab4523)); title="Departure Targets" |  |
| <a id="s-8c1a32b3c2"></a>`departures` | no | [DepartureCatalog](#s-2793192bfa) |  |
| <a id="s-4a14c949b3"></a>`observers` | no | type="object"; additionalProperties=([ObserverEndpointDocument](#s-d22a99ebb1)); title="Observers" |  |
| <a id="s-6d109dd82b"></a>`operational_state_retention_seconds` | no | type="integer"; minimum=1; default=2592000; title="Operational State Retention Seconds" |  |
| <a id="s-7460db7590"></a>`recipes` | yes | [RecipeCatalog](#s-664850f07f) |  |
| <a id="s-95d2416d35"></a>`riverhog_allow_insecure_http` | no | type="boolean"; default=false; title="Riverhog Allow Insecure Http" |  |
| <a id="s-2b5fe3caf3"></a>`riverhog_base_url` | yes | type="string"; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-5b8828b6bc"></a>`riverhog_token_file` | yes | type="string"; format="path"; title="Riverhog Token File" |  |
| <a id="s-7f257091bc"></a>`scheduler_interval_seconds` | no | type="number"; minimum=0.1; default=5; title="Scheduler Interval Seconds" |  |
| <a id="s-b26ac8ec82"></a>`target_authority_batch_size` | no | type="integer"; minimum=1; maximum=128; default=100; title="Target Authority Batch Size" |  |
| <a id="s-9668df5c46"></a>`target_callback_allow_insecure_http` | no | type="boolean"; default=false; title="Target Callback Allow Insecure Http" |  |
| <a id="s-a3cf335fe6"></a>`target_callback_base_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Target Callback Base Url" |  |
| <a id="s-6efc4bcbfb"></a>`target_callback_signing_key_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Target Callback Signing Key File" |  |
| <a id="s-d5490b4623"></a>`targets` | no | type="object"; additionalProperties=([EndpointDocument](#s-cb60ab4523)); title="Targets" |  |

### Definitions

- [AdmissionCatalog](#s-fe81d7a141)
- [AdmissionPolicy](#s-fc8565460b)
- [AllVisibleAdmissionSelector](#s-1e54b4db1c)
- [ArtifactAssociation](#s-d72b93db5b)
- [ArtifactFactBinding](#s-cd492e4dfe)
- [ArtifactRule](#s-38bb1fab85)
- [CollectionTag](#s-cc96da4653)
- [DepartureCatalog](#s-2793192bfa)
- [DeparturePolicy](#s-68fed06b31)
- [EndpointDocument](#s-cb60ab4523)
- [FactPredicate](#s-b1f87a02d6)
- [InputArtifactContract](#s-ffad33c38f)
- [JsonSchemaValidationProfile](#s-ba91ba9eb4)
- [JsonValue](#s-2765580a97)
- [NonnegativeDecimal](#s-0b17781711)
- [ObserverEndpointDocument](#s-d22a99ebb1)
- [ObserverUse](#s-4405fb2d44)
- [OperationContract](#s-b4d2a6a395)
- [OperationProjection](#s-71ba5cd2b1)
- [OutputArtifactContract](#s-079f90cad9)
- [RecipeCatalog](#s-664850f07f)
- [RecipeCoordinationRoute](#s-427b778992)
- [RecipeDefinition](#s-70d453a919)
- [RecipeIdentityRef](#s-c5e2a436bd)
- [RecipeJoin](#s-47fdf0fc29)
- [RecipeJoinMember](#s-01f994f78f)
- [RecipeRoute](#s-26cbee2736)
- [SemanticValidationProfile](#s-b24758c7f1)
- [TaggedAdmissionSelector](#s-957d73dee3)

### <a id="s-fe81d7a141"></a>definition `AdmissionCatalog`

- <a id="s-f02e0bf972"></a>`type`: `"object"`
- <a id="s-8307a18876"></a>`additionalProperties`: `false`
- <a id="s-c1bc215406"></a>`title`: `"AdmissionCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b3455ae7b"></a>`format` | no | type="string"; const="stove0-admissions/v1"; default="stove0-admissions/v1"; title="Format" |  |
| <a id="s-9479737fc2"></a>`policies` | no | type="array"; default=[]; items=([AdmissionPolicy](#s-fc8565460b)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-admission-catalog"} |  |

### <a id="s-fc8565460b"></a>definition `AdmissionPolicy`

- <a id="s-ea3187b19c"></a>`type`: `"object"`
- <a id="s-7150c857f6"></a>`additionalProperties`: `false`
- <a id="s-f88f2a90e0"></a>`description`: `"One bounded admission rule over the policy's Riverhog authorization view."`
- <a id="s-3aed0e0677"></a>`required`: `["id","revision","selector","recipe_id","recipe_revision","recipe_sha256"]`
- <a id="s-5b37dc82a7"></a>`title`: `"AdmissionPolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d513b58204"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready"; title="Automatic Preview" |  |
| <a id="s-843c5f90f5"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Effective Intent" |  |
| <a id="s-e5545f70b9"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1"; title="Format" |  |
| <a id="s-843bfbd476"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-b6a70b4785"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-6ed36e603b"></a>`recipe_revision` | yes | [NonnegativeDecimal](#s-0b17781711); ge=1 |  |
| <a id="s-32c8d80ced"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Recipe Sha256" |  |
| <a id="s-08ea6b6add"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-e90f8ffa66"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-1e54b4db1c)); ([TaggedAdmissionSelector](#s-957d73dee3))]; title="Selector" |  |

### <a id="s-1e54b4db1c"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-14d81703fc"></a>`type`: `"object"`
- <a id="s-73206d32ff"></a>`additionalProperties`: `false`
- <a id="s-5b6d65d258"></a>`title`: `"AllVisibleAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-78f3a37f4e"></a>`kind` | no | type="string"; const="all"; default="all"; title="Kind" |  |

### <a id="s-d72b93db5b"></a>definition `ArtifactAssociation`

- <a id="s-62ff051064"></a>`type`: `"object"`
- <a id="s-a0ddf41a49"></a>`additionalProperties`: `false`
- <a id="s-3c4d8f8b7e"></a>`description`: `"Associate classified artifacts without assigning device meaning to Stove0."`
- <a id="s-326a0b3884"></a>`required`: `["primary_role","associated_roles"]`
- <a id="s-afc7ecbfc4"></a>`title`: `"ArtifactAssociation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-80c8b174bf"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Associated Roles" |  |
| <a id="s-ae0b7e2709"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem"; title="Path Identity" |  |
| <a id="s-bab713c2f4"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Role" |  |

### <a id="s-cd492e4dfe"></a>definition `ArtifactFactBinding`

- <a id="s-5a05b84ab2"></a>`type`: `"object"`
- <a id="s-dd5999ed41"></a>`additionalProperties`: `false`
- <a id="s-7a26e9b991"></a>`description`: `"Locate subject-keyed records inside one observer's declared facts schema."`
- <a id="s-5b634b58bc"></a>`required`: `["records_pointer"]`
- <a id="s-0d3c41864d"></a>`title`: `"ArtifactFactBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fa4221fac6"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Artifact Id Pointer" |  |
| <a id="s-d5f0d72647"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Records Pointer" |  |

### <a id="s-38bb1fab85"></a>definition `ArtifactRule`

- <a id="s-d29b64077c"></a>`type`: `"object"`
- <a id="s-04de394fd4"></a>`additionalProperties`: `false`
- <a id="s-77d304ce55"></a>`description`: `"Classify one path; first matching rule wins."`
- <a id="s-572884772f"></a>`title`: `"ArtifactRule"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ca8c7209da"></a>`glob` | no | type="string"; default="*"; title="Glob" |  |
| <a id="s-53caadfcf5"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-6f24688ea8"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-cc96da4653"></a>definition `CollectionTag`

- <a id="s-e1c767d0de"></a>`type`: `"string"`
- <a id="s-e6be069769"></a>`maxLength`: `65536`
- <a id="s-78bfc2c02b"></a>`minLength`: `1`
- <a id="s-811f8cd78a"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-d24042bce9"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-16ffcfb8c5"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-2793192bfa"></a>definition `DepartureCatalog`

- <a id="s-34a2a7a42a"></a>`type`: `"object"`
- <a id="s-87bace37e3"></a>`additionalProperties`: `false`
- <a id="s-26b9e49a91"></a>`title`: `"DepartureCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57b896b8f1"></a>`format` | no | type="string"; const="stove0-departures/v1"; default="stove0-departures/v1"; title="Format" |  |
| <a id="s-bf1a8c8e3e"></a>`policies` | no | type="array"; default=[]; items=([DeparturePolicy](#s-68fed06b31)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-departure-catalog"} |  |

### <a id="s-68fed06b31"></a>definition `DeparturePolicy`

- <a id="s-0fddc48c11"></a>`type`: `"object"`
- <a id="s-da8adc9b09"></a>`additionalProperties`: `false`
- <a id="s-73bf3679a0"></a>`description`: `"A catalog departure subscription with no recipe or artifact authority."`
- <a id="s-db67c497dd"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`
- <a id="s-515f5fc0fc"></a>`title`: `"DeparturePolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c74447c232"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1"; title="Format" |  |
| <a id="s-f4932511b4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-d8a044bad1"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-76264125d8"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-1e54b4db1c)); ([TaggedAdmissionSelector](#s-957d73dee3))]; title="Selector" |  |
| <a id="s-b60e83241a"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |
| <a id="s-7f8f9149f3"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1; title="Target Registration Id" |  |

### <a id="s-cb60ab4523"></a>definition `EndpointDocument`

- <a id="s-586f7be19c"></a>`type`: `"object"`
- <a id="s-964f9606f5"></a>`additionalProperties`: `false`
- <a id="s-177304af0c"></a>`required`: `["base_url"]`
- <a id="s-a313c27086"></a>`title`: `"EndpointDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ff97cdbf1"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-30e4b5cf1f"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-b29a3f24e9"></a>`token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Token File" |  |

### <a id="s-b1f87a02d6"></a>definition `FactPredicate`

- <a id="s-d5572fb862"></a>`type`: `"object"`
- <a id="s-000d5d00da"></a>`additionalProperties`: `false`
- <a id="s-4bda92c393"></a>`required`: `["observation_contract_id","pointer"]`
- <a id="s-f7446a0c57"></a>`title`: `"FactPredicate"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ae0395fca8"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-cd492e4dfe)); (type="null")]; default=null |  |
| <a id="s-7649a0e5d1"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Artifact Roles" |  |
| <a id="s-81c8244d62"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observation Contract Id" |  |
| <a id="s-249f6f3dbb"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals"; title="Operator" |  |
| <a id="s-6023ab045e"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Pointer" |  |
| <a id="s-291ab4bf38"></a>`value` | no | [JsonValue](#s-2765580a97); default=null |  |

### <a id="s-ffad33c38f"></a>definition `InputArtifactContract`

- <a id="s-77ba280cff"></a>`type`: `"object"`
- <a id="s-f09523b689"></a>`additionalProperties`: `false`
- <a id="s-ad3218adb7"></a>`required`: `["role"]`
- <a id="s-0bdb2383ea"></a>`title`: `"InputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-664371d07a"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null; title="Allowed Dispositions" |  |
| <a id="s-8f607bc874"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-1a17ff09af"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-c917f45a0f"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-ba91ba9eb4"></a>definition `JsonSchemaValidationProfile`

- <a id="s-1bb4c39e02"></a>`type`: `"object"`
- <a id="s-f786784903"></a>`additionalProperties`: `false`
- <a id="s-1e1b49e636"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-2cef1943c7"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1e98bc4e5"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-9bd90d2ea4"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-9ad91afaa3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-4a32d1da28"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-f486b1ac85"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Schema" |  |

### <a id="s-2765580a97"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-0b17781711"></a>definition `NonnegativeDecimal`

- <a id="s-42b657aae6"></a>`type`: `"string"`
- <a id="s-2ecb84854a"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-d22a99ebb1"></a>definition `ObserverEndpointDocument`

- <a id="s-c2d53c55ac"></a>`type`: `"object"`
- <a id="s-30bef9832e"></a>`additionalProperties`: `false`
- <a id="s-6cb63e0cbe"></a>`required`: `["base_url"]`
- <a id="s-66d08932d5"></a>`title`: `"ObserverEndpointDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6958c6fa9"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-dcf4edaae2"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-d7a68dd8b2"></a>`semantic_validator_providers` | no | type="array"; default=[]; items=(type="string"); title="Semantic Validator Providers" |  |
| <a id="s-311ae05637"></a>`token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Token File" |  |

### <a id="s-4405fb2d44"></a>definition `ObserverUse`

- <a id="s-7c7cd33aba"></a>`type`: `"object"`
- <a id="s-8e910d3e70"></a>`additionalProperties`: `false`
- <a id="s-3252e08277"></a>`required`: `["registration_id","contract_id","contract_sha256"]`
- <a id="s-d8ffd76868"></a>`title`: `"ObserverUse"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-aa935111fb"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38bb1fab85)); title="Artifact Rules" |  |
| <a id="s-bd4c427216"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Contract Id" |  |
| <a id="s-927e6f8f94"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-1252cb251d"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-a7cc4059d2"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Options" |  |
| <a id="s-facd27bff3"></a>`registration_id` | yes | type="string"; title="Registration Id" |  |
| <a id="s-58d26da596"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-f9371972e0"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |

### <a id="s-b4d2a6a395"></a>definition `OperationContract`

- <a id="s-748afd919e"></a>`type`: `"object"`
- <a id="s-e7ac856b71"></a>`additionalProperties`: `false`
- <a id="s-63e897417b"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`
- <a id="s-6875b52f04"></a>`title`: `"OperationContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-48e7d3d541"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-fae1286324"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-ba91ba9eb4)); (type="null")]; default=null |  |
| <a id="s-01ad78ca9c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-352dd20cf0"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-ffad33c38f)); minItems=1; title="Inputs" |  |
| <a id="s-9c261ebc70"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-ba91ba9eb4) |  |
| <a id="s-93da0a2775"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-b24758c7f1) |  |
| <a id="s-203a3174b9"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-079f90cad9)); title="Outputs" |  |
| <a id="s-2ef5584064"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-1a78f28f9b"></a>`source_collection_retirement_permitted` | no | type="boolean"; default=false; title="Source Collection Retirement Permitted" |  |

### <a id="s-71ba5cd2b1"></a>definition `OperationProjection`

- <a id="s-4c821fb008"></a>`type`: `"object"`
- <a id="s-90e943dcd5"></a>`additionalProperties`: `false`
- <a id="s-ebb22ee03b"></a>`description`: `"One declarative JSON-pointer copy into an operation request."`
- <a id="s-2c851a2fb7"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`
- <a id="s-529f8e2cbd"></a>`title`: `"OperationProjection"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bec826bc23"></a>`destination` | yes | type="string"; enum=["intent","target-options"]; title="Destination" |  |
| <a id="s-df210d5cf0"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Destination Pointer" |  |
| <a id="s-c837439712"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"]; title="Source" |  |
| <a id="s-d392e59096"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Source Pointer" |  |

### <a id="s-079f90cad9"></a>definition `OutputArtifactContract`

- <a id="s-fd134ccd29"></a>`type`: `"object"`
- <a id="s-c6ac093de7"></a>`additionalProperties`: `false`
- <a id="s-87951de663"></a>`required`: `["role","derived_from_roles"]`
- <a id="s-a2d2ad5eae"></a>`title`: `"OutputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-13dc10b79b"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Derived From Roles" |  |
| <a id="s-b87866d337"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-0efb427ddd"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-9d4d6b71d1"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-664850f07f"></a>definition `RecipeCatalog`

- <a id="s-ac47e5a79b"></a>`type`: `"object"`
- <a id="s-02eafb7225"></a>`additionalProperties`: `false`
- <a id="s-0a7d978235"></a>`required`: `["operations","recipes"]`
- <a id="s-1e540f02b5"></a>`title`: `"RecipeCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1aabb5fb1"></a>`format` | no | type="string"; const="stove0-recipes/v1"; default="stove0-recipes/v1"; title="Format" |  |
| <a id="s-6aaad3721e"></a>`operations` | yes | type="array"; items=([OperationContract](#s-b4d2a6a395)); title="Operations" |  |
| <a id="s-2193924248"></a>`recipes` | yes | type="array"; items=([RecipeDefinition](#s-70d453a919)); title="Recipes" |  |

### <a id="s-427b778992"></a>definition `RecipeCoordinationRoute`

- <a id="s-22aa3ab3b7"></a>`type`: `"object"`
- <a id="s-b38f8b7125"></a>`additionalProperties`: `false`
- <a id="s-a81c094015"></a>`description`: `"One exact subrecipe selected as a branch-bound coordinator."`
- <a id="s-2a0506a39d"></a>`required`: `["id","recipe"]`
- <a id="s-f6e1a48ba9"></a>`title`: `"RecipeCoordinationRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cbebe9a051"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38bb1fab85)); title="Artifact Rules" |  |
| <a id="s-755380c97e"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-9f39522bcf"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-2097b434ef"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Intent" |  |
| <a id="s-8e9a181c1d"></a>`kind` | no | type="string"; const="coordination"; default="coordination"; title="Kind" |  |
| <a id="s-3c6fff178c"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-9f225e8d85"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-71ba5cd2b1)); title="Projections" |  |
| <a id="s-bb4d0b911f"></a>`recipe` | yes | [RecipeIdentityRef](#s-c5e2a436bd) |  |
| <a id="s-66792fae54"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-b1f87a02d6)); title="When" |  |

### <a id="s-70d453a919"></a>definition `RecipeDefinition`

- <a id="s-2b1875db95"></a>`type`: `"object"`
- <a id="s-7ffaac5469"></a>`additionalProperties`: `false`
- <a id="s-3c45adec53"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`
- <a id="s-36877b876b"></a>`title`: `"RecipeDefinition"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0f10b3432"></a>`allow_derived_inputs` | no | type="boolean"; default=false; title="Allow Derived Inputs" |  |
| <a id="s-a5bb005adc"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-d72b93db5b)); title="Artifact Associations" |  |
| <a id="s-8ca12f70ac"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection"; title="Event Input Closure" |  |
| <a id="s-18adb6efa8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-6a0ae1038f"></a>`join` | no | anyOf=[([RecipeJoin](#s-47fdf0fc29)); (type="null")]; default=null |  |
| <a id="s-0acf934c9e"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-4405fb2d44)); title="Observers" |  |
| <a id="s-8e8cc3c3d2"></a>`revision` | yes | [NonnegativeDecimal](#s-0b17781711); ge=1 |  |
| <a id="s-3802bef0c6"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-26cbee2736)); ([RecipeCoordinationRoute](#s-427b778992))]); minItems=1; title="Routes" |  |
| <a id="s-1052382fcf"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-30619b108c"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-a3d9e9dd4f"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"]; title="Unmatched Artifact Disposition" |  |

### <a id="s-c5e2a436bd"></a>definition `RecipeIdentityRef`

- <a id="s-1081a2910c"></a>`type`: `"object"`
- <a id="s-04d78168a3"></a>`additionalProperties`: `false`
- <a id="s-a562f19dc0"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-fee12260ac"></a>`required`: `["id","revision","sha256"]`
- <a id="s-1afc292726"></a>`title`: `"RecipeIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d31fd31c38"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-ab82ac5e33"></a>`revision` | yes | [NonnegativeDecimal](#s-0b17781711); ge=1 |  |
| <a id="s-338ffa3027"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-47fdf0fc29"></a>definition `RecipeJoin`

- <a id="s-339ac7fa0f"></a>`type`: `"object"`
- <a id="s-6dfec69131"></a>`additionalProperties`: `false`
- <a id="s-56d7233ca9"></a>`required`: `["id","members","operation_id","target_registration_id"]`
- <a id="s-0bd6b8814b"></a>`title`: `"RecipeJoin"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1579099489"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-d8e5b2794c"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-2810cb2fd7"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Intent" |  |
| <a id="s-67d1384a0b"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-01f994f78f)); minItems=2; title="Members" |  |
| <a id="s-783b3e2b59"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-d85e418e9a"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-71ba5cd2b1)); title="Projections" |  |
| <a id="s-eb0a2fc6cf"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Target Options" |  |
| <a id="s-530d424ad5"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |

### <a id="s-01f994f78f"></a>definition `RecipeJoinMember`

- <a id="s-cf7a24cd26"></a>`type`: `"object"`
- <a id="s-f30229b307"></a>`additionalProperties`: `false`
- <a id="s-1fda67d204"></a>`required`: `["branch_id","output_roles"]`
- <a id="s-e39f3c92c1"></a>`title`: `"RecipeJoinMember"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-08c2cff080"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-c3289ab8ed"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Output Roles" |  |

### <a id="s-26cbee2736"></a>definition `RecipeRoute`

- <a id="s-3252dcdcc9"></a>`type`: `"object"`
- <a id="s-a407337318"></a>`additionalProperties`: `false`
- <a id="s-a1d0d5b719"></a>`description`: `"One ordinary target/effect leaf selected by a recipe."`
- <a id="s-289eace792"></a>`required`: `["id","operation_id","target_registration_id"]`
- <a id="s-b6890343b5"></a>`title`: `"RecipeRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f89669d942"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-38bb1fab85)); title="Artifact Rules" |  |
| <a id="s-dbe70020ba"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-ffb223dc22"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-528e5f4966"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-46eef006be"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Intent" |  |
| <a id="s-d938a55860"></a>`kind` | no | type="string"; const="operation"; default="operation"; title="Kind" |  |
| <a id="s-e5cd085a8a"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-570bb01430"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-037f6ee2be"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-71ba5cd2b1)); title="Projections" |  |
| <a id="s-af98969318"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-2765580a97)); title="Target Options" |  |
| <a id="s-e72e54f2ec"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |
| <a id="s-b10d91eb2d"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-b1f87a02d6)); title="When" |  |

### <a id="s-b24758c7f1"></a>definition `SemanticValidationProfile`

- <a id="s-3ce490f1a0"></a>`type`: `"object"`
- <a id="s-ec4484bf5d"></a>`additionalProperties`: `false`
- <a id="s-c96971c8f7"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-cb7007bb55"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d67a62dc43"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-3537dd3730"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-4a4ed48a85"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-d3c4b53f88"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### <a id="s-957d73dee3"></a>definition `TaggedAdmissionSelector`

- <a id="s-06c37c2323"></a>`type`: `"object"`
- <a id="s-a7866b8711"></a>`additionalProperties`: `false`
- <a id="s-63d469dfde"></a>`required`: `["required"]`
- <a id="s-b109d3cc4a"></a>`title`: `"TaggedAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7fa970a90d"></a>`kind` | no | type="string"; const="tags"; default="tags"; title="Kind" |  |
| <a id="s-63f8d2952c"></a>`required` | yes | type="array"; items=([CollectionTag](#s-cc96da4653)); maxItems=100; minItems=1; title="Required"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-server:configuration:stove0-document"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionPolicy · field effective_intent](#s-843c5f90f5) | `cardinality · entries · operational_policy` | shared above |
| [definition ArtifactAssociation · field associated_roles](#s-80c8b174bf) | `cardinality · items · operational_policy` | shared above |
| [definition FactPredicate · field artifact_roles](#s-7649a0e5d1) | `cardinality · items · operational_policy` | shared above |
| <a id="s-9df1350367"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-664371d07a) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-f486b1ac85) | `cardinality · entries · operational_policy` | shared above |
| [definition ObserverEndpointDocument · field semantic_validator_providers](#s-d7a68dd8b2) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverUse · field artifact_rules](#s-aa935111fb) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverUse · field options](#s-a7cc4059d2) | `cardinality · entries · operational_policy` | shared above |
| [definition OperationContract · field inputs](#s-352dd20cf0) | `cardinality · items · operational_policy` | shared above |
| [definition OperationContract · field outputs](#s-203a3174b9) | `cardinality · items · operational_policy` | shared above |
| [definition OutputArtifactContract · field derived_from_roles](#s-13dc10b79b) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCatalog · field operations](#s-6aaad3721e) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCatalog · field recipes](#s-2193924248) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field artifact_rules](#s-cbebe9a051) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field associated_roles](#s-755380c97e) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field intent](#s-2097b434ef) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field projections](#s-9f225e8d85) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field when](#s-66792fae54) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field artifact_associations](#s-a5bb005adc) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field observers](#s-0acf934c9e) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field routes](#s-3802bef0c6) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field intent](#s-2810cb2fd7) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoin · field members](#s-67d1384a0b) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field projections](#s-d85e418e9a) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field target_options](#s-eb0a2fc6cf) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoinMember · field output_roles](#s-c3289ab8ed) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field artifact_rules](#s-f89669d942) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field associated_roles](#s-dbe70020ba) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field intent](#s-46eef006be) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field projections](#s-037f6ee2be) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field target_options](#s-af98969318) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field when](#s-b10d91eb2d) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticValidationProfile · field rules](#s-d3c4b53f88) | `cardinality · items · operational_policy` | shared above |
| [field departure_targets](#s-df3397f280) | `cardinality · entries · operational_policy` | shared above |
| [field observers](#s-4a14c949b3) | `cardinality · entries · operational_policy` | shared above |
| [field targets](#s-d5490b4623) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionCatalog · field policies](#s-9479737fc2) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-admission-catalog" |
| [definition AdmissionPolicy · field recipe_id](#s-b6a70b4785) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition AdmissionPolicy · field recipe_sha256](#s-32c8d80ced) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionTag](#s-cc96da4653) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-cc96da4653) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| [definition DepartureCatalog · field policies](#s-bf1a8c8e3e) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-departure-catalog" |
| [definition DeparturePolicy · field target_identity](#s-b60e83241a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition DeparturePolicy · field target_registration_id](#s-7f8f9149f3) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-4a32d1da28) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field contract_sha256](#s-927e6f8f94) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field maximum_result_bytes](#s-1252cb251d) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ObserverUse · field timeout_seconds](#s-f9371972e0) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition OperationContract · field contract_sha256](#s-48e7d3d541) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition RecipeIdentityRef · field sha256](#s-338ffa3027) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-c6dcdc2f35"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-d67a62dc43) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SemanticValidationProfile · field profile_sha256](#s-4a4ed48a85) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TaggedAdmissionSelector · field required](#s-63f8d2952c) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |
| [field target_authority_batch_size](#s-b26ac8ec82) | `value · schema-value · contract_max` | maximum=128; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5073d0fbcc"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-979a1ffae9"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-4ab0197277"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-server:configuration:stove0-document](../../../evidence/sources/authorities.md#src-9431b80de8) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::Stove0Document](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/stove0-server:configuration:stove0-document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bb16f97c9b8855dd99940d830683da8d7279cf2de9cfb935786c0ecfbe7c927 -->

```json
{
  "$defs": {
    "AdmissionCatalog": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-admissions/v1",
          "default": "stove0-admissions/v1",
          "title": "Format",
          "type": "string"
        },
        "policies": {
          "default": [],
          "items": {
            "$ref": "#/$defs/AdmissionPolicy"
          },
          "maxItems": 100,
          "title": "Policies",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-deployment-admission-catalog"
          }
        }
      },
      "title": "AdmissionCatalog",
      "type": "object"
    },
    "AdmissionPolicy": {
      "additionalProperties": false,
      "description": "One bounded admission rule over the policy's Riverhog authorization view.",
      "properties": {
        "automatic_preview": {
          "const": "accept-ready",
          "default": "accept-ready",
          "title": "Automatic Preview",
          "type": "string"
        },
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Effective Intent",
          "type": "object"
        },
        "format": {
          "const": "stove0-admission-policy/v1",
          "default": "stove0-admission-policy/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Recipe Id",
          "type": "string"
        },
        "recipe_revision": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "recipe_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Recipe Sha256",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "selector": {
          "discriminator": {
            "mapping": {
              "all": "#/$defs/AllVisibleAdmissionSelector",
              "tags": "#/$defs/TaggedAdmissionSelector"
            },
            "propertyName": "kind"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/AllVisibleAdmissionSelector"
            },
            {
              "$ref": "#/$defs/TaggedAdmissionSelector"
            }
          ],
          "title": "Selector"
        }
      },
      "required": [
        "id",
        "revision",
        "selector",
        "recipe_id",
        "recipe_revision",
        "recipe_sha256"
      ],
      "title": "AdmissionPolicy",
      "type": "object"
    },
    "AllVisibleAdmissionSelector": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "all",
          "default": "all",
          "title": "Kind",
          "type": "string"
        }
      },
      "title": "AllVisibleAdmissionSelector",
      "type": "object"
    },
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
    "CollectionTag": {
      "maxLength": 65536,
      "minLength": 1,
      "type": "string",
      "x-riverhog-encoded-bytes-max": 65536,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-human-authored-collection-tag"
      },
      "x-unicode-normalization": "NFC"
    },
    "DepartureCatalog": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-departures/v1",
          "default": "stove0-departures/v1",
          "title": "Format",
          "type": "string"
        },
        "policies": {
          "default": [],
          "items": {
            "$ref": "#/$defs/DeparturePolicy"
          },
          "maxItems": 100,
          "title": "Policies",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-deployment-departure-catalog"
          }
        }
      },
      "title": "DepartureCatalog",
      "type": "object"
    },
    "DeparturePolicy": {
      "additionalProperties": false,
      "description": "A catalog departure subscription with no recipe or artifact authority.",
      "properties": {
        "format": {
          "const": "stove0-departure-policy/v1",
          "default": "stove0-departure-policy/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "selector": {
          "discriminator": {
            "mapping": {
              "all": "#/$defs/AllVisibleAdmissionSelector",
              "tags": "#/$defs/TaggedAdmissionSelector"
            },
            "propertyName": "kind"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/AllVisibleAdmissionSelector"
            },
            {
              "$ref": "#/$defs/TaggedAdmissionSelector"
            }
          ],
          "title": "Selector"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Target Registration Id",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "selector",
        "target_registration_id",
        "target_identity"
      ],
      "title": "DeparturePolicy",
      "type": "object"
    },
    "EndpointDocument": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "token_file": {
          "anyOf": [
            {
              "format": "path",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Token File"
        }
      },
      "required": [
        "base_url"
      ],
      "title": "EndpointDocument",
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
    "NonnegativeDecimal": {
      "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
      "type": "string"
    },
    "ObserverEndpointDocument": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "semantic_validator_providers": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Semantic Validator Providers",
          "type": "array"
        },
        "token_file": {
          "anyOf": [
            {
              "format": "path",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Token File"
        }
      },
      "required": [
        "base_url"
      ],
      "title": "ObserverEndpointDocument",
      "type": "object"
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
    "RecipeCatalog": {
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
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
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
    },
    "TaggedAdmissionSelector": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "tags",
          "default": "tags",
          "title": "Kind",
          "type": "string"
        },
        "required": {
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "maxItems": 100,
          "minItems": 1,
          "title": "Required",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-exact-classification-admission-predicate"
          }
        }
      },
      "required": [
        "required"
      ],
      "title": "TaggedAdmissionSelector",
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "admissions": {
      "$ref": "#/$defs/AdmissionCatalog"
    },
    "api_token_file": {
      "anyOf": [
        {
          "format": "path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Api Token File"
    },
    "browse_token_lifetime_seconds": {
      "default": 86400,
      "minimum": 1,
      "title": "Browse Token Lifetime Seconds",
      "type": "integer"
    },
    "browse_token_signing_key_file": {
      "format": "path",
      "title": "Browse Token Signing Key File",
      "type": "string"
    },
    "capability_ttl_seconds": {
      "default": 900,
      "minimum": 30,
      "title": "Capability Ttl Seconds",
      "type": "integer"
    },
    "claim_lease_seconds": {
      "default": 1800,
      "minimum": 30,
      "title": "Claim Lease Seconds",
      "type": "integer"
    },
    "database_url_file": {
      "format": "path",
      "title": "Database Url File",
      "type": "string"
    },
    "declared_workspace_protection": {
      "enum": [
        "encrypted-at-rest",
        "memory-backed"
      ],
      "title": "Declared Workspace Protection",
      "type": "string"
    },
    "departure_targets": {
      "additionalProperties": {
        "$ref": "#/$defs/EndpointDocument"
      },
      "title": "Departure Targets",
      "type": "object"
    },
    "departures": {
      "$ref": "#/$defs/DepartureCatalog"
    },
    "observers": {
      "additionalProperties": {
        "$ref": "#/$defs/ObserverEndpointDocument"
      },
      "title": "Observers",
      "type": "object"
    },
    "operational_state_retention_seconds": {
      "default": 2592000,
      "minimum": 1,
      "title": "Operational State Retention Seconds",
      "type": "integer"
    },
    "recipes": {
      "$ref": "#/$defs/RecipeCatalog"
    },
    "riverhog_allow_insecure_http": {
      "default": false,
      "title": "Riverhog Allow Insecure Http",
      "type": "boolean"
    },
    "riverhog_base_url": {
      "minLength": 1,
      "title": "Riverhog Base Url",
      "type": "string"
    },
    "riverhog_token_file": {
      "format": "path",
      "title": "Riverhog Token File",
      "type": "string"
    },
    "scheduler_interval_seconds": {
      "default": 5,
      "minimum": 0.1,
      "title": "Scheduler Interval Seconds",
      "type": "number"
    },
    "target_authority_batch_size": {
      "default": 100,
      "maximum": 128,
      "minimum": 1,
      "title": "Target Authority Batch Size",
      "type": "integer"
    },
    "target_callback_allow_insecure_http": {
      "default": false,
      "title": "Target Callback Allow Insecure Http",
      "type": "boolean"
    },
    "target_callback_base_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Target Callback Base Url"
    },
    "target_callback_signing_key_file": {
      "anyOf": [
        {
          "format": "path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Target Callback Signing Key File"
    },
    "targets": {
      "additionalProperties": {
        "$ref": "#/$defs/EndpointDocument"
      },
      "title": "Targets",
      "type": "object"
    }
  },
  "required": [
    "database_url_file",
    "riverhog_base_url",
    "riverhog_token_file",
    "recipes",
    "declared_workspace_protection",
    "browse_token_signing_key_file"
  ],
  "title": "Stove0Document",
  "type": "object"
}
```

</details>
