# Stove0Document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:stove0-server:stove0document:65d2f97e94 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-d288d4a179"></a>

- <a id="s-42c10e1616"></a>`type`: `"object"`
- <a id="s-5c5241ac13"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json"`
- <a id="s-68aacab627"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-7308fdf240"></a>`additionalProperties`: `false`
- <a id="s-1457c36b07"></a>`required`: `["database_url_file","riverhog_base_url","riverhog_token_file","recipes","declared_workspace_protection","browse_token_signing_key_file"]`
- <a id="s-d58a4e27af"></a>`title`: `"Stove0Document"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b7d5bd9960"></a>`admissions` | no | [AdmissionCatalog](#s-77e7f49733) |  |
| <a id="s-884d8844b7"></a>`api_token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Api Token File" |  |
| <a id="s-5493860107"></a>`browse_token_lifetime_seconds` | no | type="integer"; minimum=1; default=86400; title="Browse Token Lifetime Seconds" |  |
| <a id="s-882ac4dddb"></a>`browse_token_signing_key_file` | yes | type="string"; format="path"; title="Browse Token Signing Key File" |  |
| <a id="s-c428f8e48e"></a>`capability_ttl_seconds` | no | type="integer"; minimum=30; default=900; title="Capability Ttl Seconds" |  |
| <a id="s-4b59b7a574"></a>`claim_lease_seconds` | no | type="integer"; minimum=30; default=1800; title="Claim Lease Seconds" |  |
| <a id="s-b8896a7a53"></a>`database_url_file` | yes | type="string"; format="path"; title="Database Url File" |  |
| <a id="s-12030cb131"></a>`declared_workspace_protection` | yes | type="string"; enum=["encrypted-at-rest","memory-backed"]; title="Declared Workspace Protection" |  |
| <a id="s-722780c480"></a>`departure_targets` | no | type="object"; additionalProperties=([EndpointDocument](#s-eae6adeac5)); title="Departure Targets" |  |
| <a id="s-73f59ec352"></a>`departures` | no | [DepartureCatalog](#s-8315ec45fa) |  |
| <a id="s-c203982f2e"></a>`observers` | no | type="object"; additionalProperties=([ObserverEndpointDocument](#s-3a204b3ad3)); title="Observers" |  |
| <a id="s-afecb9295a"></a>`operational_state_retention_seconds` | no | type="integer"; minimum=1; default=2592000; title="Operational State Retention Seconds" |  |
| <a id="s-492dd9d73e"></a>`recipes` | yes | [RecipeCatalog](#s-a329a21ff6) |  |
| <a id="s-40e075f1af"></a>`riverhog_allow_insecure_http` | no | type="boolean"; default=false; title="Riverhog Allow Insecure Http" |  |
| <a id="s-3ea37197e7"></a>`riverhog_base_url` | yes | type="string"; minLength=1; title="Riverhog Base Url" |  |
| <a id="s-94da1c378c"></a>`riverhog_token_file` | yes | type="string"; format="path"; title="Riverhog Token File" |  |
| <a id="s-044bed11de"></a>`scheduler_interval_seconds` | no | type="number"; minimum=0.1; default=5; title="Scheduler Interval Seconds" |  |
| <a id="s-e930ae1ff1"></a>`target_authority_batch_size` | no | type="integer"; minimum=1; maximum=128; default=100; title="Target Authority Batch Size" |  |
| <a id="s-d11056320b"></a>`target_callback_allow_insecure_http` | no | type="boolean"; default=false; title="Target Callback Allow Insecure Http" |  |
| <a id="s-90d46b8f8c"></a>`target_callback_base_url` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Target Callback Base Url" |  |
| <a id="s-dffe5349db"></a>`target_callback_signing_key_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Target Callback Signing Key File" |  |
| <a id="s-2442403930"></a>`targets` | no | type="object"; additionalProperties=([EndpointDocument](#s-eae6adeac5)); title="Targets" |  |

### Definitions

- [AdmissionCatalog](#s-77e7f49733)
- [AdmissionPolicy](#s-3c0a20ec2a)
- [AllVisibleAdmissionSelector](#s-4f8e6a0ae1)
- [ArtifactAssociation](#s-74fa4a3fab)
- [ArtifactFactBinding](#s-5b778c28d4)
- [ArtifactRule](#s-e706f6d538)
- [CollectionTag](#s-d51448a8f3)
- [DepartureCatalog](#s-8315ec45fa)
- [DeparturePolicy](#s-e684e2b97c)
- [EndpointDocument](#s-eae6adeac5)
- [FactPredicate](#s-98bda761aa)
- [InputArtifactContract](#s-63cd3e5861)
- [JsonSchemaValidationProfile](#s-bc04606520)
- [JsonValue](#s-b5176e5691)
- [NonnegativeDecimal](#s-0d996c28c5)
- [ObserverEndpointDocument](#s-3a204b3ad3)
- [ObserverUse](#s-a305740044)
- [OperationContract](#s-99246ce9b1)
- [OperationProjection](#s-6bdeb7adc9)
- [OutputArtifactContract](#s-879a855a76)
- [RecipeCatalog](#s-a329a21ff6)
- [RecipeCoordinationRoute](#s-a68ae022b7)
- [RecipeDefinition](#s-2713b9211b)
- [RecipeIdentityRef](#s-4dfc1a756d)
- [RecipeJoin](#s-ed744f11c6)
- [RecipeJoinMember](#s-275fbcf962)
- [RecipeRoute](#s-093de2556c)
- [SemanticValidationProfile](#s-90f15dbfc0)
- [TaggedAdmissionSelector](#s-72c6e8176d)

### <a id="s-77e7f49733"></a>definition `AdmissionCatalog`

- <a id="s-d0d2613b5c"></a>`type`: `"object"`
- <a id="s-c92179381d"></a>`additionalProperties`: `false`
- <a id="s-d05e811da2"></a>`title`: `"AdmissionCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e3c203ffca"></a>`format` | no | type="string"; const="stove0-admissions/v1"; default="stove0-admissions/v1"; title="Format" |  |
| <a id="s-0b8ec15fed"></a>`policies` | no | type="array"; default=[]; items=([AdmissionPolicy](#s-3c0a20ec2a)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-admission-catalog"} |  |

### <a id="s-3c0a20ec2a"></a>definition `AdmissionPolicy`

- <a id="s-cd0575c3a0"></a>`type`: `"object"`
- <a id="s-f39714c973"></a>`additionalProperties`: `false`
- <a id="s-905b216368"></a>`description`: `"One bounded admission rule over the policy's Riverhog authorization view."`
- <a id="s-0c0e6949bf"></a>`required`: `["id","revision","selector","recipe_id","recipe_revision","recipe_sha256"]`
- <a id="s-470115d955"></a>`title`: `"AdmissionPolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7be182e3d5"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready"; title="Automatic Preview" |  |
| <a id="s-689bd12bdd"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Effective Intent" |  |
| <a id="s-2114d7571c"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1"; title="Format" |  |
| <a id="s-5cc8e5e2f2"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-96468e3f72"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1; title="Recipe Id" |  |
| <a id="s-68841ae5e0"></a>`recipe_revision` | yes | [NonnegativeDecimal](#s-0d996c28c5); ge=1 |  |
| <a id="s-33d9227a13"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Recipe Sha256" |  |
| <a id="s-d73ac23b5b"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-8b57c13234"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-4f8e6a0ae1)); ([TaggedAdmissionSelector](#s-72c6e8176d))]; title="Selector" |  |

### <a id="s-4f8e6a0ae1"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-c17416ced6"></a>`type`: `"object"`
- <a id="s-aa1bc1c8b5"></a>`additionalProperties`: `false`
- <a id="s-75b9b45b87"></a>`title`: `"AllVisibleAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bcc458bebe"></a>`kind` | no | type="string"; const="all"; default="all"; title="Kind" |  |

### <a id="s-74fa4a3fab"></a>definition `ArtifactAssociation`

- <a id="s-72c4c71bdc"></a>`type`: `"object"`
- <a id="s-6f495fee1a"></a>`additionalProperties`: `false`
- <a id="s-87971ba152"></a>`description`: `"Associate classified artifacts without assigning device meaning to Stove0."`
- <a id="s-ff09382fc0"></a>`required`: `["primary_role","associated_roles"]`
- <a id="s-d855eef1f5"></a>`title`: `"ArtifactAssociation"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b7e92cb537"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Associated Roles" |  |
| <a id="s-54d05f0bb9"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem"; title="Path Identity" |  |
| <a id="s-d6bb2d112e"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Primary Role" |  |

### <a id="s-5b778c28d4"></a>definition `ArtifactFactBinding`

- <a id="s-13b27b727e"></a>`type`: `"object"`
- <a id="s-c420529e3e"></a>`additionalProperties`: `false`
- <a id="s-04451f30c7"></a>`description`: `"Locate subject-keyed records inside one observer's declared facts schema."`
- <a id="s-0fedd41cf2"></a>`required`: `["records_pointer"]`
- <a id="s-13665bc1ff"></a>`title`: `"ArtifactFactBinding"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-932a98f8bf"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Artifact Id Pointer" |  |
| <a id="s-902a87713d"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Records Pointer" |  |

### <a id="s-e706f6d538"></a>definition `ArtifactRule`

- <a id="s-bc41e5c49f"></a>`type`: `"object"`
- <a id="s-4785aa167e"></a>`additionalProperties`: `false`
- <a id="s-c49cfef052"></a>`description`: `"Classify one path; first matching rule wins."`
- <a id="s-0aed73931d"></a>`title`: `"ArtifactRule"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8cf9bfb5de"></a>`glob` | no | type="string"; default="*"; title="Glob" |  |
| <a id="s-9f2cf7a8cc"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null; title="Media Type" |  |
| <a id="s-55ab2a0b53"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-d51448a8f3"></a>definition `CollectionTag`

- <a id="s-1515cdf091"></a>`type`: `"string"`
- <a id="s-c155bc5cc1"></a>`maxLength`: `65536`
- <a id="s-62ed6dfde6"></a>`minLength`: `1`
- <a id="s-bfd8295dea"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-06866ffe82"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-a2a107758b"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-8315ec45fa"></a>definition `DepartureCatalog`

- <a id="s-13ccfce995"></a>`type`: `"object"`
- <a id="s-5684617880"></a>`additionalProperties`: `false`
- <a id="s-84261ba148"></a>`title`: `"DepartureCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df74a482c0"></a>`format` | no | type="string"; const="stove0-departures/v1"; default="stove0-departures/v1"; title="Format" |  |
| <a id="s-920fdcf1fb"></a>`policies` | no | type="array"; default=[]; items=([DeparturePolicy](#s-e684e2b97c)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-departure-catalog"} |  |

### <a id="s-e684e2b97c"></a>definition `DeparturePolicy`

- <a id="s-e41950069e"></a>`type`: `"object"`
- <a id="s-ee314a1ae8"></a>`additionalProperties`: `false`
- <a id="s-3e0515ac3f"></a>`description`: `"A catalog departure subscription with no recipe or artifact authority."`
- <a id="s-13ebb128ac"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`
- <a id="s-15cfc6691e"></a>`title`: `"DeparturePolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1245bfe2a"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1"; title="Format" |  |
| <a id="s-854fa90185"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-6093e1fc27"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-64282ec006"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-4f8e6a0ae1)); ([TaggedAdmissionSelector](#s-72c6e8176d))]; title="Selector" |  |
| <a id="s-dfb5eb334a"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |
| <a id="s-41eb0bd276"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1; title="Target Registration Id" |  |

### <a id="s-eae6adeac5"></a>definition `EndpointDocument`

- <a id="s-fcad607e12"></a>`type`: `"object"`
- <a id="s-fbbec8db7c"></a>`additionalProperties`: `false`
- <a id="s-c43b05873e"></a>`required`: `["base_url"]`
- <a id="s-26b40a147a"></a>`title`: `"EndpointDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50eb249df6"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-b52e1b1482"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-25aeabd477"></a>`token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Token File" |  |

### <a id="s-98bda761aa"></a>definition `FactPredicate`

- <a id="s-e06890fae5"></a>`type`: `"object"`
- <a id="s-32f3bf5f68"></a>`additionalProperties`: `false`
- <a id="s-0dcc0d566e"></a>`required`: `["observation_contract_id","pointer"]`
- <a id="s-936011b384"></a>`title`: `"FactPredicate"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75594e1dfd"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-5b778c28d4)); (type="null")]; default=null |  |
| <a id="s-4665b82d55"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Artifact Roles" |  |
| <a id="s-5f102efd44"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observation Contract Id" |  |
| <a id="s-9321958d37"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals"; title="Operator" |  |
| <a id="s-6f91c279bd"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Pointer" |  |
| <a id="s-66a5729579"></a>`value` | no | [JsonValue](#s-b5176e5691); default=null |  |

### <a id="s-63cd3e5861"></a>definition `InputArtifactContract`

- <a id="s-0078315ceb"></a>`type`: `"object"`
- <a id="s-26a1b5ceb7"></a>`additionalProperties`: `false`
- <a id="s-5efeb85e5a"></a>`required`: `["role"]`
- <a id="s-51db4ffb23"></a>`title`: `"InputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6be0701001"></a>`allowed_dispositions` | no | anyOf=[(type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"])); (type="null")]; default=null; title="Allowed Dispositions" |  |
| <a id="s-ecf87835a1"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-81318f0d03"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-a7baf9f9a1"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-bc04606520"></a>definition `JsonSchemaValidationProfile`

- <a id="s-f2549b9260"></a>`type`: `"object"`
- <a id="s-2f5f0b6465"></a>`additionalProperties`: `false`
- <a id="s-f0bf5f1f32"></a>`required`: `["id","profile_sha256","schema"]`
- <a id="s-81b0b48a4d"></a>`title`: `"JsonSchemaValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-417c98670a"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema"; title="Dialect" |  |
| <a id="s-e39833018e"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only"; title="Format Policy" |  |
| <a id="s-fdee503ad5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-33d736204b"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-b821449124"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Schema" |  |

### <a id="s-b5176e5691"></a>definition `JsonValue`

- Accepts: any JSON value.

### <a id="s-0d996c28c5"></a>definition `NonnegativeDecimal`

- <a id="s-f08d4b0123"></a>`type`: `"string"`
- <a id="s-9c973175fe"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

### <a id="s-3a204b3ad3"></a>definition `ObserverEndpointDocument`

- <a id="s-6f68acb278"></a>`type`: `"object"`
- <a id="s-84d7a565b4"></a>`additionalProperties`: `false`
- <a id="s-128102e686"></a>`required`: `["base_url"]`
- <a id="s-ca44563092"></a>`title`: `"ObserverEndpointDocument"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b8a4416412"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-2625fa5d53"></a>`base_url` | yes | type="string"; minLength=1; title="Base Url" |  |
| <a id="s-6061ff960e"></a>`semantic_validator_providers` | no | type="array"; default=[]; items=(type="string"); title="Semantic Validator Providers" |  |
| <a id="s-7950d888fd"></a>`token_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Token File" |  |

### <a id="s-a305740044"></a>definition `ObserverUse`

- <a id="s-3338d33466"></a>`type`: `"object"`
- <a id="s-909d89346b"></a>`additionalProperties`: `false`
- <a id="s-2809522309"></a>`required`: `["registration_id","contract_id","contract_sha256"]`
- <a id="s-bfccec73e8"></a>`title`: `"ObserverUse"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-33095d2791"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-e706f6d538)); title="Artifact Rules" |  |
| <a id="s-1bcccef6eb"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Contract Id" |  |
| <a id="s-01942e197e"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-8cd7d085da"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576; title="Maximum Result Bytes" |  |
| <a id="s-1e3b28c964"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Options" |  |
| <a id="s-f69fd7fb17"></a>`registration_id` | yes | type="string"; title="Registration Id" |  |
| <a id="s-a9d8f03a91"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Retrieval Policy" |  |
| <a id="s-67d38e4507"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300; title="Timeout Seconds" |  |

### <a id="s-99246ce9b1"></a>definition `OperationContract`

- <a id="s-9b99f79109"></a>`type`: `"object"`
- <a id="s-dbfddaf9aa"></a>`additionalProperties`: `false`
- <a id="s-12cfa93442"></a>`required`: `["id","intent_schema","intent_semantics","inputs","contract_sha256"]`
- <a id="s-34d3b8d1d2"></a>`title`: `"OperationContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3e8600ab8e"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Contract Sha256" |  |
| <a id="s-17f421f23f"></a>`effect_receipt_schema` | no | anyOf=[([JsonSchemaValidationProfile](#s-bc04606520)); (type="null")]; default=null |  |
| <a id="s-3183d29523"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-572a4258e8"></a>`inputs` | yes | type="array"; items=([InputArtifactContract](#s-63cd3e5861)); minItems=1; title="Inputs" |  |
| <a id="s-693961f80a"></a>`intent_schema` | yes | [JsonSchemaValidationProfile](#s-bc04606520) |  |
| <a id="s-8246cb6384"></a>`intent_semantics` | yes | [SemanticValidationProfile](#s-90f15dbfc0) |  |
| <a id="s-5909bc8861"></a>`outputs` | no | type="array"; default=[]; items=([OutputArtifactContract](#s-879a855a76)); title="Outputs" |  |
| <a id="s-72491031f1"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-48264b03a7"></a>`source_collection_retirement_permitted` | no | type="boolean"; default=false; title="Source Collection Retirement Permitted" |  |

### <a id="s-6bdeb7adc9"></a>definition `OperationProjection`

- <a id="s-6977dd85f7"></a>`type`: `"object"`
- <a id="s-3ee53fb84c"></a>`additionalProperties`: `false`
- <a id="s-93137dd119"></a>`description`: `"One declarative JSON-pointer copy into an operation request."`
- <a id="s-10ab1f14bd"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`
- <a id="s-e284dc018c"></a>`title`: `"OperationProjection"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e1890a622c"></a>`destination` | yes | type="string"; enum=["intent","target-options"]; title="Destination" |  |
| <a id="s-f0971b4e05"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Destination Pointer" |  |
| <a id="s-8ce6293df8"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"]; title="Source" |  |
| <a id="s-cfc8588eec"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Source Pointer" |  |

### <a id="s-879a855a76"></a>definition `OutputArtifactContract`

- <a id="s-d1d6c9a92e"></a>`type`: `"object"`
- <a id="s-7a736694cc"></a>`additionalProperties`: `false`
- <a id="s-fbbf93bef6"></a>`required`: `["role","derived_from_roles"]`
- <a id="s-b0393b834f"></a>`title`: `"OutputArtifactContract"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-df21c5bab0"></a>`derived_from_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Derived From Roles" |  |
| <a id="s-7b5c18db54"></a>`maximum` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum" |  |
| <a id="s-5299be53dc"></a>`minimum` | no | type="integer"; minimum=0; default=1; title="Minimum" |  |
| <a id="s-c98a59e5ee"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |

### <a id="s-a329a21ff6"></a>definition `RecipeCatalog`

- <a id="s-f220dac633"></a>`type`: `"object"`
- <a id="s-ef08b2c99e"></a>`additionalProperties`: `false`
- <a id="s-a1c699b7fb"></a>`required`: `["operations","recipes"]`
- <a id="s-222e95b428"></a>`title`: `"RecipeCatalog"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb7703fc15"></a>`format` | no | type="string"; const="stove0-recipes/v1"; default="stove0-recipes/v1"; title="Format" |  |
| <a id="s-efb36cedd7"></a>`operations` | yes | type="array"; items=([OperationContract](#s-99246ce9b1)); title="Operations" |  |
| <a id="s-29c18b24a1"></a>`recipes` | yes | type="array"; items=([RecipeDefinition](#s-2713b9211b)); title="Recipes" |  |

### <a id="s-a68ae022b7"></a>definition `RecipeCoordinationRoute`

- <a id="s-17a52a26ec"></a>`type`: `"object"`
- <a id="s-3f6c5fc3eb"></a>`additionalProperties`: `false`
- <a id="s-adbbf4405f"></a>`description`: `"One exact subrecipe selected as a branch-bound coordinator."`
- <a id="s-9dd9825dae"></a>`required`: `["id","recipe"]`
- <a id="s-8bb5d61cb2"></a>`title`: `"RecipeCoordinationRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-635105f6d2"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-e706f6d538)); title="Artifact Rules" |  |
| <a id="s-4d674084d7"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-2c0f0eadc1"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-7c56bc2579"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Intent" |  |
| <a id="s-d315308448"></a>`kind` | no | type="string"; const="coordination"; default="coordination"; title="Kind" |  |
| <a id="s-a4a288dce0"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-fc7155f60c"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-6bdeb7adc9)); title="Projections" |  |
| <a id="s-fcfec54aff"></a>`recipe` | yes | [RecipeIdentityRef](#s-4dfc1a756d) |  |
| <a id="s-af8060972d"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-98bda761aa)); title="When" |  |

### <a id="s-2713b9211b"></a>definition `RecipeDefinition`

- <a id="s-b5a0dc7ccf"></a>`type`: `"object"`
- <a id="s-9703ccf025"></a>`additionalProperties`: `false`
- <a id="s-18cf5231e0"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`
- <a id="s-05114eec08"></a>`title`: `"RecipeDefinition"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2814b0f1d9"></a>`allow_derived_inputs` | no | type="boolean"; default=false; title="Allow Derived Inputs" |  |
| <a id="s-315815ce4c"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-74fa4a3fab)); title="Artifact Associations" |  |
| <a id="s-fa535f3aba"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection"; title="Event Input Closure" |  |
| <a id="s-1f317f6d93"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-19720ea474"></a>`join` | no | anyOf=[([RecipeJoin](#s-ed744f11c6)); (type="null")]; default=null |  |
| <a id="s-d9d2900ffa"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-a305740044)); title="Observers" |  |
| <a id="s-4c1f256119"></a>`revision` | yes | [NonnegativeDecimal](#s-0d996c28c5); ge=1 |  |
| <a id="s-56985bc3f6"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-093de2556c)); ([RecipeCoordinationRoute](#s-a68ae022b7))]); minItems=1; title="Routes" |  |
| <a id="s-2b26536fae"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-ed6805633e"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-e45b0fa891"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"]; title="Unmatched Artifact Disposition" |  |

### <a id="s-4dfc1a756d"></a>definition `RecipeIdentityRef`

- <a id="s-ad5eb3c3c3"></a>`type`: `"object"`
- <a id="s-c1eff807bf"></a>`additionalProperties`: `false`
- <a id="s-b468d57c22"></a>`description`: `"Embedded Stove0 reference to the Riverhog recipe identity."`
- <a id="s-7b6edf6d9d"></a>`required`: `["id","revision","sha256"]`
- <a id="s-68f49e371e"></a>`title`: `"RecipeIdentityRef"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d725bac0f6"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-f12316b0a7"></a>`revision` | yes | [NonnegativeDecimal](#s-0d996c28c5); ge=1 |  |
| <a id="s-df4bd1b442"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### <a id="s-ed744f11c6"></a>definition `RecipeJoin`

- <a id="s-9552c69a0e"></a>`type`: `"object"`
- <a id="s-9a067b734b"></a>`additionalProperties`: `false`
- <a id="s-6d1a553b25"></a>`required`: `["id","members","operation_id","target_registration_id"]`
- <a id="s-f5dbb09bb9"></a>`title`: `"RecipeJoin"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee17f14c37"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-8b3c6b0834"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-2f5224f498"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Intent" |  |
| <a id="s-d870a61098"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-275fbcf962)); minItems=2; title="Members" |  |
| <a id="s-5545ecc932"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-0b2cd1bd0b"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-6bdeb7adc9)); title="Projections" |  |
| <a id="s-09ce341ccc"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Target Options" |  |
| <a id="s-799c9285e7"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |

### <a id="s-275fbcf962"></a>definition `RecipeJoinMember`

- <a id="s-7c33538367"></a>`type`: `"object"`
- <a id="s-2d9a7e32ec"></a>`additionalProperties`: `false`
- <a id="s-76ed328e62"></a>`required`: `["branch_id","output_roles"]`
- <a id="s-6a52eafc6c"></a>`title`: `"RecipeJoinMember"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-abfac1cfe0"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-5ae626d714"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Output Roles" |  |

### <a id="s-093de2556c"></a>definition `RecipeRoute`

- <a id="s-cc5ac15044"></a>`type`: `"object"`
- <a id="s-3a1700c513"></a>`additionalProperties`: `false`
- <a id="s-cb4be765b9"></a>`description`: `"One ordinary target/effect leaf selected by a recipe."`
- <a id="s-3145fcb788"></a>`required`: `["id","operation_id","target_registration_id"]`
- <a id="s-cb1751b9af"></a>`title`: `"RecipeRoute"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7071f811cd"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-e706f6d538)); title="Artifact Rules" |  |
| <a id="s-53e40bc102"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Associated Roles" |  |
| <a id="s-389ff570bc"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-5ab0d425d9"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-5118ce4484"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Intent" |  |
| <a id="s-f39d8a232d"></a>`kind` | no | type="string"; const="operation"; default="operation"; title="Kind" |  |
| <a id="s-f13e04160c"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Operation Id" |  |
| <a id="s-46192f4fc9"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null; title="Primary Role" |  |
| <a id="s-f90e6b388c"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-6bdeb7adc9)); title="Projections" |  |
| <a id="s-6761443358"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-b5176e5691)); title="Target Options" |  |
| <a id="s-2275ae18ea"></a>`target_registration_id` | yes | type="string"; title="Target Registration Id" |  |
| <a id="s-c363e1e22c"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-98bda761aa)); title="When" |  |

### <a id="s-90f15dbfc0"></a>definition `SemanticValidationProfile`

- <a id="s-117a29c689"></a>`type`: `"object"`
- <a id="s-ad6dffb04e"></a>`additionalProperties`: `false`
- <a id="s-54c8aa5584"></a>`required`: `["id","rules","profile_sha256"]`
- <a id="s-b866ef472d"></a>`title`: `"SemanticValidationProfile"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-85fe204c17"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Conformance Vectors Sha256" |  |
| <a id="s-9f45b8401d"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-970078e960"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Profile Sha256" |  |
| <a id="s-510b688426"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1; title="Rules" |  |

### <a id="s-72c6e8176d"></a>definition `TaggedAdmissionSelector`

- <a id="s-ade79f62e3"></a>`type`: `"object"`
- <a id="s-8528865670"></a>`additionalProperties`: `false`
- <a id="s-50c6256b35"></a>`required`: `["required"]`
- <a id="s-b1539ef732"></a>`title`: `"TaggedAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2eaead4e39"></a>`kind` | no | type="string"; const="tags"; default="tags"; title="Kind" |  |
| <a id="s-52dacba67d"></a>`required` | yes | type="array"; items=([CollectionTag](#s-d51448a8f3)); maxItems=100; minItems=1; title="Required"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionPolicy · field effective_intent](#s-689bd12bdd) | `cardinality · entries · operational_policy` | shared above |
| [definition ArtifactAssociation · field associated_roles](#s-b7e92cb537) | `cardinality · items · operational_policy` | shared above |
| [definition FactPredicate · field artifact_roles](#s-4665b82d55) | `cardinality · items · operational_policy` | shared above |
| <a id="s-9658482b21"></a>[definition InputArtifactContract · field allowed_dispositions · array value](#s-6be0701001) | `cardinality · items · operational_policy` | shared above |
| [definition JsonSchemaValidationProfile · field schema](#s-b821449124) | `cardinality · entries · operational_policy` | shared above |
| [definition ObserverEndpointDocument · field semantic_validator_providers](#s-6061ff960e) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverUse · field artifact_rules](#s-33095d2791) | `cardinality · items · operational_policy` | shared above |
| [definition ObserverUse · field options](#s-1e3b28c964) | `cardinality · entries · operational_policy` | shared above |
| [definition OperationContract · field inputs](#s-572a4258e8) | `cardinality · items · operational_policy` | shared above |
| [definition OperationContract · field outputs](#s-5909bc8861) | `cardinality · items · operational_policy` | shared above |
| [definition OutputArtifactContract · field derived_from_roles](#s-df21c5bab0) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCatalog · field operations](#s-efb36cedd7) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCatalog · field recipes](#s-29c18b24a1) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field artifact_rules](#s-635105f6d2) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field associated_roles](#s-4d674084d7) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field intent](#s-7c56bc2579) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field projections](#s-fc7155f60c) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeCoordinationRoute · field when](#s-af8060972d) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field artifact_associations](#s-315815ce4c) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field observers](#s-d9d2900ffa) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeDefinition · field routes](#s-56985bc3f6) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field intent](#s-2f5224f498) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoin · field members](#s-d870a61098) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field projections](#s-0b2cd1bd0b) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeJoin · field target_options](#s-09ce341ccc) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeJoinMember · field output_roles](#s-5ae626d714) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field artifact_rules](#s-7071f811cd) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field associated_roles](#s-53e40bc102) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field intent](#s-5118ce4484) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field projections](#s-f90e6b388c) | `cardinality · items · operational_policy` | shared above |
| [definition RecipeRoute · field target_options](#s-6761443358) | `cardinality · entries · operational_policy` | shared above |
| [definition RecipeRoute · field when](#s-c363e1e22c) | `cardinality · items · operational_policy` | shared above |
| [definition SemanticValidationProfile · field rules](#s-510b688426) | `cardinality · items · operational_policy` | shared above |
| [field departure_targets](#s-722780c480) | `cardinality · entries · operational_policy` | shared above |
| [field observers](#s-c203982f2e) | `cardinality · entries · operational_policy` | shared above |
| [field targets](#s-2442403930) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdmissionCatalog · field policies](#s-0b8ec15fed) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-admission-catalog" |
| [definition AdmissionPolicy · field recipe_id](#s-96468e3f72) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition AdmissionPolicy · field recipe_sha256](#s-33d9227a13) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition CollectionTag](#s-d51448a8f3) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-d51448a8f3) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| [definition DepartureCatalog · field policies](#s-920fdcf1fb) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-departure-catalog" |
| [definition DeparturePolicy · field target_identity](#s-dfb5eb334a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition DeparturePolicy · field target_registration_id](#s-41eb0bd276) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition JsonSchemaValidationProfile · field profile_sha256](#s-33d736204b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field contract_sha256](#s-01942e197e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObserverUse · field maximum_result_bytes](#s-8cd7d085da) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [definition ObserverUse · field timeout_seconds](#s-67d38e4507) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| [definition OperationContract · field contract_sha256](#s-3e8600ab8e) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition RecipeIdentityRef · field sha256](#s-df4bd1b442) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3824bdd92f"></a>[definition SemanticValidationProfile · field conformance_vectors_sha256 · string value](#s-85fe204c17) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SemanticValidationProfile · field profile_sha256](#s-970078e960) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition TaggedAdmissionSelector · field required](#s-52dacba67d) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |
| [field target_authority_batch_size](#s-e930ae1ff1) | `value · schema-value · contract_max` | maximum=128; minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-5033e8ce7b"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-4b672935cb"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-678dcf0dae"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/stove0-server.schema.json](../../../evidence/sources/authorities.md#src-303a07bbf7) — [some-implementations/stove0/application/server/src/stove0\_core/config.schema.json](../../../../../../some-implementations/stove0/application/server/src/stove0_core/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1stove0-server.schema.json`

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
