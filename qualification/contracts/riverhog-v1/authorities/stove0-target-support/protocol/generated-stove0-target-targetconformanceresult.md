# generated:stove0-target: TargetConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-target-support:generated-stove0-target-targetconformanceresult:4818b112f4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-3862b77c4ff3) |
| Contract elements | 1 |
| Extent decisions | 133 |

## External contract

<a id="s-8cf797b25e0c"></a>
- <a id="s-166034031e87"></a>`title`: TargetConformanceResult
- <a id="s-83a0e7889f20"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-54849ef8eaac"></a>`coverage` | yes | #/$defs/TargetConformanceCoverage |  |
| <a id="s-073c47c5d8f4"></a>`format` | no | type="string"; const="stove0-target-conformance-result/v1" |  |
| <a id="s-cdbf79ac63b1"></a>`operation_evidence` | no | type="array"; items=(#/$defs/TargetOperationConformanceEvidence) |  |
| <a id="s-b7d24726ce82"></a>`operations` | yes | type="array"; items=(#/$defs/TargetOperationConformance) |  |
| <a id="s-65f3100125b1"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"] |  |
| <a id="s-25e202ab55a3"></a>`target` | yes | #/$defs/TargetContract |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-0993620c2ac4"></a>`AcceptedTargetJob` | type="object"; fields=`declaration`, `request_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-64c14db0d1d3"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-4348e1d25c8d"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-9650f4fcc099"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-5d02bd6d337d"></a>`BranchWorkBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `decision_sha256`, `kind`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-40a59539e2ba"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-3b0dd8e93b7c"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-b707894e8c44"></a>`ControllerEvidence` | type="object"; fields=`controller_evidence_sha256`, `execution_envelope`, `format`; additional keys=`additionalProperties`, `required` |
| <a id="s-ef86be7809d5"></a>`EffectPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-06c04695a537"></a>`EvaluationBinding` | type="object"; fields=`evaluation_id`, `matrix_sha256`, `parameters`, `variant_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c944b74fe4e"></a>`ExecutionEnvelope` | type="object"; fields=`claim_id`, `execution_envelope_sha256`, `fence`, `format`, `target_plan`, `workflow_plan`; additional keys=`additionalProperties`, `required` |
| <a id="s-91999efbe249"></a>`ExternalEffectReceipt` | type="object"; fields=`execution_sha256`, `format`, `job_id`, `operation_contract_sha256`, `plan_sha256`, `receipt_sha256`, `request_sha256`, `result`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-ae1b42ecf667"></a>`InputArtifactContract` | type="object"; fields=`allowed_dispositions`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-13aa1fbe2e3d"></a>`JoinWorkBinding` | type="object"; fields=`branch_set_sha256`, `kind`, `members`, `parent_work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-b0603b619301"></a>`JoinWorkMemberBinding` | type="object"; fields=`artifact_selection_sha256`, `branch_id`, `producer_settlement_sha256`, `settlement_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-9f3395149907"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-40b30ec57567"></a>`JsonValue` | empty object |
| <a id="s-60b8c95972ff"></a>`ObservationEvidence` | type="object"; fields=`request`, `result`; additional keys=`additionalProperties`, `required` |
| <a id="s-5dbd59498f30"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-69a7b8b45ebd"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-445cbd01979e"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-50894448b7cc"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-df8b23cd9810"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-0d00de964e61"></a>`OperationContract` | type="object"; fields=`contract_sha256`, `effect_receipt_schema`, `id`, `inputs`, `intent_schema`, `intent_semantics`, `outputs`, `result_kind`, `source_retirement_permitted`; additional keys=`additionalProperties`, `required` |
| <a id="s-46180fa7c37f"></a>`OperationRef` | type="object"; fields=`id`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d338c851cc21"></a>`OutputArtifactContract` | type="object"; fields=`derived_from_roles`, `maximum`, `minimum`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-deff193b8ede"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-fc2841caccd4"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-7a55ca8250f0"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-175e50de1874"></a>`RecipeRef` | type="object"; fields=`id`, `revision`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-7bceecb5d977"></a>`SemanticIntentConformanceVector` | type="object"; fields=`accepted`, `id`, `intent`; additional keys=`additionalProperties`, `required` |
| <a id="s-f23536131655"></a>`SemanticIntentConformanceVectors` | type="object"; fields=`format`, `profile_id`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-ee0cfd3dd1a8"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |
| <a id="s-3af4423fcb3c"></a>`TargetConformanceCoverage` | type="object"; fields=`advertised`, `complete`, `exercised`; additional keys=`additionalProperties`, `required` |
| <a id="s-3f7cbc3a29fc"></a>`TargetContract` | type="object"; fields=`contract_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `operations`, `protocol`, `source_revision`, `transport`; additional keys=`additionalProperties`, `required` |
| <a id="s-633d457279e2"></a>`TargetExecutionEvidence` | type="object"; fields=`execution_sha256`, `operation_contract_sha256`, `plan_sha256`, `runtime`, `target_contract_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-c0bd76e74fc4"></a>`TargetFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-d1d84486e097"></a>`TargetInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-d47194a8edb5"></a>`TargetInputAuthority` | type="object"; fields=`roles`, `selection`; additional keys=`additionalProperties`, `required` |
| <a id="s-a279c59e726e"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-cd86ac44a9e8"></a>`TargetJobDeclaration` | type="object"; fields=`claim_id`, `controller_evidence`, `fence`, `job_id`, `plan`, `workspace_assurance`; additional keys=`additionalProperties`, `required` |
| <a id="s-ef307f8c1469"></a>`TargetJobStatus` | type="object"; fields=`attempt`, `derivation`, `effect_receipt`, `execution_evidence`, `failure`, `inapplicable`, `job_id`, `output_collection`, `plan_sha256`, `production`, `progress`, `protocol`, `request_sha256`, `state`; allOf=additional keys=`else`, `if`, `then`; additional keys=`additionalProperties`, `required` |
| <a id="s-d56ff4bc3bbd"></a>`TargetOperationConformance` | type="object"; fields=`intent_semantics_conformance_vectors_sha256`, `intent_semantics_id`, `intent_semantics_sha256`, `operation_contract_sha256`, `operation_id`, `options_schema_sha256`, `result_kind`, `semantic_conformance`; additional keys=`additionalProperties`, `required` |
| <a id="s-fd16c1183e14"></a>`TargetOperationConformanceEvidence` | type="object"; fields=`accepted_job`, `job_status`, `operation`, `operation_id`, `preflight`, `preflight_request`, `semantic_conformance`, `submission`; additional keys=`additionalProperties`, `required` |
| <a id="s-bf993b5ad198"></a>`TargetOperationSupport` | type="object"; fields=`operation_contract_sha256`, `operation_id`, `options_schema`, `result_kind`; additional keys=`additionalProperties`, `required` |
| <a id="s-51289b901efe"></a>`TargetPlanBinding` | type="object"; fields=`operation_contract_sha256`, `plan`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-53a4ad89c585"></a>`TargetPreflightRequest` | type="object"; fields=`inputs`, `intent`, `observations`, `operation_contract_sha256`, `operation_id`, `protocol`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-e0e566c7c91e"></a>`TargetPreflightResponse` | type="object"; fields=`plan`, `target`; additional keys=`additionalProperties`, `required` |
| <a id="s-1c359015195f"></a>`TargetProductionAuthority` | type="object"; fields=`disposition_count`, `disposition_sha256`, `format`, `job_id`, `outputs`, `plan_sha256`, `production_sha256`, `riverhog_disposition_set`, `source_edge_count`, `source_edge_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d3a358b3637f"></a>`TargetProgress` | type="object"; fields=`completed`, `phase`, `total`, `unit`; additional keys=`additionalProperties`, `required` |
| <a id="s-b21311b603c7"></a>`TargetSemanticConformance` | type="object"; fields=`accepted_vector_ids`, `conformance_vectors_sha256`, `profile_id`, `profile_sha256`, `rejected_vector_ids`, `status`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-db60c6557235"></a>`TransformPlan` | type="object"; fields=`inputs`, `intent`, `observation_result_sha256s`, `operation_contract_sha256`, `operation_id`, `plan_sha256`, `protocol`, `target_contract_sha256`, `target_implementation_id`, `target_options`; additional keys=`additionalProperties`, `required` |
| <a id="s-cf67f00f31f8"></a>`WorkIdentity` | type="object"; fields=`effective_intent`, `evaluation`, `fork_join`, `format`, `inputs`, `recipe`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-3c1466782882"></a>`WorkflowPlan` | type="object"; fields=`format`, `input_retrieval_policy`, `observations`, `operation`, `output_policy`, `requested_target_options`, `result_kind`, `retirement_grace_seconds`, `retirement_policy`, `target_contract_sha256`, `target_registration_id`, `work`, `workflow_plan_sha256`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-target-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field operation_evidence](#s-cdbf79ac63b1) | `cardinality · items · operational_policy` | shared above |
| [field operations](#s-b7d24726ce82) | `cardinality · items · operational_policy` | shared above |
| <a id="s-0b7b61ad78b7"></a>definition ArtifactSubject · field bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-9ca967882cd0"></a>definition EffectPlan · field intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-5b72f76c589c"></a>definition EffectPlan · field observation_result_sha256s | `cardinality · items · operational_policy` | shared above |
| <a id="s-43205d72a8fb"></a>definition EffectPlan · field target_options | `cardinality · entries · operational_policy` | shared above |
| <a id="s-c87e02bb0d15"></a>definition EvaluationBinding · field parameters | `cardinality · entries · operational_policy` | shared above |
| <a id="s-9b803bf4d5a7"></a>definition ExternalEffectReceipt · field result | `cardinality · entries · operational_policy` | shared above |
| <a id="s-5084ea7e1946"></a>definition JoinWorkBinding · field members | `cardinality · items · operational_policy` | shared above |
| <a id="s-939fc744e7c2"></a>definition ObservationRequest · field options | `cardinality · entries · operational_policy` | shared above |
| <a id="s-44546f41ce20"></a>definition ObservationRequest · field subjects | `cardinality · items · operational_policy` | shared above |
| <a id="s-b7c76ef4ab0e"></a>definition ObservationResult · field execution_evidence | `cardinality · entries · operational_policy` | shared above |
| <a id="s-5ebe3db283a6"></a>definition ObservationResult · field facts · anyOf alternative 1 | `cardinality · entries · operational_policy` | shared above |
| <a id="s-8538d5e49f5e"></a>definition ObservationResult · field subjects | `cardinality · items · operational_policy` | shared above |
| <a id="s-864b2f62ff47"></a>definition OperationContract · field inputs | `cardinality · items · operational_policy` | shared above |
| <a id="s-9fda8d7f177e"></a>definition OperationContract · field outputs | `cardinality · items · operational_policy` | shared above |
| <a id="s-8e3fe95a0f9d"></a>definition OutputArtifactRoleCount · field count | `value · schema-value · operational_policy` | shared above |
| <a id="s-220278cd969b"></a>definition OutputArtifactSetIdentity · field roles | `cardinality · items · operational_policy` | shared above |
| <a id="s-0158a31d7a3b"></a>definition SemanticIntentConformanceVector · field intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-91e52e6fb9ba"></a>definition SemanticIntentConformanceVectors · field vectors | `cardinality · items · operational_policy` | shared above |
| <a id="s-1c9046d10a1b"></a>definition TargetContract · field operations | `cardinality · items · operational_policy` | shared above |
| <a id="s-6112224187c4"></a>definition TargetExecutionEvidence · field runtime | `cardinality · entries · operational_policy` | shared above |
| <a id="s-ea2d4c93fb3c"></a>definition TargetInputAuthority · field roles | `cardinality · items · operational_policy` | shared above |
| <a id="s-209e85a56d0d"></a>definition TargetInputRoleCount · field count | `value · schema-value · operational_policy` | shared above |
| <a id="s-548bd052fb97"></a>definition TargetJobStatus · field derivation · anyOf alternative 1 | `cardinality · entries · operational_policy` | shared above |
| <a id="s-483659871f8e"></a>definition TargetPlanBinding · field plan | `cardinality · entries · operational_policy` | shared above |
| <a id="s-1ec262eca2f9"></a>definition TargetPreflightRequest · field intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-1c0ca85fa07e"></a>definition TargetPreflightRequest · field observations | `cardinality · items · operational_policy` | shared above |
| <a id="s-7ca71e93a2f2"></a>definition TargetPreflightRequest · field target_options | `cardinality · entries · operational_policy` | shared above |
| <a id="s-875d411fa0fc"></a>definition TargetSemanticConformance · field accepted_vector_ids | `cardinality · items · operational_policy` | shared above |
| <a id="s-fa13b84a6bee"></a>definition TargetSemanticConformance · field rejected_vector_ids | `cardinality · items · operational_policy` | shared above |
| <a id="s-4a16960d1fa9"></a>definition TransformPlan · field intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-be4f54821b8d"></a>definition TransformPlan · field observation_result_sha256s | `cardinality · items · operational_policy` | shared above |
| <a id="s-64403125d5af"></a>definition TransformPlan · field target_options | `cardinality · entries · operational_policy` | shared above |
| <a id="s-fdafcd6296e8"></a>definition WorkIdentity · field effective_intent | `cardinality · entries · operational_policy` | shared above |
| <a id="s-c17fdffda5b7"></a>definition WorkIdentity · field inputs | `cardinality · items · operational_policy` | shared above |
| <a id="s-cfc3faa0e23d"></a>definition WorkflowPlan · field observations | `cardinality · items · operational_policy` | shared above |
| <a id="s-8379dee316fd"></a>definition WorkflowPlan · field output_policy | `cardinality · entries · operational_policy` | shared above |
| <a id="s-765ca1361e5c"></a>definition WorkflowPlan · field requested_target_options | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-af9b296ddb4e"></a>definition AcceptedTargetJob · field request_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f99ecb544f17"></a>definition ArtifactSelectionRef · field selection_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-aa209e506a79"></a>definition ArtifactSubject · field media_type · anyOf alternative 1 | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-d319e35eb461"></a>definition ArtifactSubject · field path | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-12d6b41438fe"></a>definition ArtifactSubject · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6bd334a851dc"></a>definition BranchWorkBinding · field artifact_selection_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4b8416247059"></a>definition BranchWorkBinding · field decision_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a0e4f9b3159c"></a>definition BranchWorkBinding · field parent_work_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-d73966d812e3"></a>definition CollectionRootRef · field archive_root_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-35151e0ed6dd"></a>definition CollectionRootRef · field content_identity | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-bd2dec1b4b3e"></a>definition ControllerEvidence · field controller_evidence_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e2ed80f39800"></a>definition EffectPlan · field observation_result_sha256s · items | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7de112878942"></a>definition EffectPlan · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-80f86a6953ad"></a>definition EffectPlan · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7309bc31e7b0"></a>definition EffectPlan · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-431d09d7ae46"></a>definition EvaluationBinding · field evaluation_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-17b04f0a6c8c"></a>definition EvaluationBinding · field matrix_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-d75b74756745"></a>definition ExecutionEnvelope · field claim_id | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-7aa4348aed61"></a>definition ExecutionEnvelope · field execution_envelope_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-db819eede729"></a>definition ExternalEffectReceipt · field execution_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-83346264eeae"></a>definition ExternalEffectReceipt · field job_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-da963ca073e2"></a>definition ExternalEffectReceipt · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-c03b774088d9"></a>definition ExternalEffectReceipt · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e8b63a720717"></a>definition ExternalEffectReceipt · field receipt_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f5b5db1c49fa"></a>definition ExternalEffectReceipt · field request_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ExternalEffectReceipt · field result](#s-9b803bf4d5a7) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-external-effect-receipt"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| <a id="s-646dbb8b9e29"></a>definition ExternalEffectReceipt · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3fd72a2def33"></a>definition JoinWorkBinding · field branch_set_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-48e066958590"></a>definition JoinWorkBinding · field parent_work_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-88bc1ecb63fe"></a>definition JoinWorkMemberBinding · field artifact_selection_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-31332e38ef05"></a>definition JoinWorkMemberBinding · field producer_settlement_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8165c6e0afe7"></a>definition JoinWorkMemberBinding · field settlement_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-42cced266682"></a>definition ObservationFailure · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-3b822180ca89"></a>definition ObservationInapplicable · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-d34e843a5f5a"></a>definition ObservationRequest · field maximum_result_bytes | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| <a id="s-e22e1377a3b9"></a>definition ObservationRequest · field observer_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-ffbfdfd0699d"></a>definition ObservationRequest · field observer_descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-cec528aff0d5"></a>definition ObservationRequest · field request_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-dbd0b6427d24"></a>definition ObservationRequest · field timeout_seconds | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |
| <a id="s-923b984c83ec"></a>definition ObservationRequest · field work_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-1b3f35e5d3de"></a>definition ObservationResult · field facts_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-55aa18ee5c41"></a>definition ObservationResult · field observer_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a0d1abd370f7"></a>definition ObservationResult · field request_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6f329e8221ae"></a>definition ObservationResult · field result_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-dae02c352934"></a>definition ObserverImplementation · field descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-c65c6661a9d2"></a>definition ObserverImplementation · field source_revision | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| <a id="s-cbd5c59a7922"></a>definition ObserverImplementation · field version | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-db12b8f13040"></a>definition OperationContract · field contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6debe79b3ab6"></a>definition OperationRef · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-a8be724a77e4"></a>definition OutputArtifactSetIdentity · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-69bab181a173"></a>definition OutputCollectionRef · field archive_root_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-7bc373dcee8e"></a>definition OutputCollectionRef · field content_identity | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-c536e7cde36e"></a>definition OutputCollectionRef · field derivation_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-21ab0ccfd9c1"></a>definition RecipeRef · field sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-82534eafff75"></a>definition TargetContract · field contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8748750d5492"></a>definition TargetContract · field image_digest | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-dce0a87d05f4"></a>definition TargetContract · field implementation_version | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-141df9c35708"></a>definition TargetContract · field source_revision | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| <a id="s-2cc5345db534"></a>definition TargetExecutionEvidence · field execution_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-626228478865"></a>definition TargetExecutionEvidence · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-1e30ff802c07"></a>definition TargetExecutionEvidence · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-654ebaa25d35"></a>definition TargetExecutionEvidence · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-07583e604aec"></a>definition TargetFailure · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-d9508e393e62"></a>definition TargetInapplicable · field message | `length · characters · contract_max` | maximum=1000; minimum=1; reason="schema-maximum" |
| <a id="s-d518182a9c83"></a>definition TargetJobDeclaration · field claim_id | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| <a id="s-09a9d64cdc5b"></a>definition TargetJobDeclaration · field job_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-ef8ef1fd2020"></a>definition TargetJobStatus · field job_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6f47f4e26aa5"></a>definition TargetJobStatus · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-9070a362f736"></a>definition TargetJobStatus · field request_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4298a4626ef4"></a>definition TargetOperationConformance · field intent_semantics_conformance_vectors_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-730db5906ae7"></a>definition TargetOperationConformance · field intent_semantics_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3bf21a25eb63"></a>definition TargetOperationConformance · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-13c5b43d972a"></a>definition TargetOperationConformance · field options_schema_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-933c5a1e3751"></a>definition TargetOperationSupport · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-891e76459752"></a>definition TargetPlanBinding · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-41e0eec4b479"></a>definition TargetPlanBinding · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0ddd68f83ad4"></a>definition TargetPlanBinding · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-58b6a5935b53"></a>definition TargetPreflightRequest · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6cbdb7e5a7ad"></a>definition TargetProductionAuthority · field disposition_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-173d7b1276a9"></a>definition TargetProductionAuthority · field job_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-fe8c9ec0748f"></a>definition TargetProductionAuthority · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0e3f79af3729"></a>definition TargetProductionAuthority · field production_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-54785aae6565"></a>definition TargetProductionAuthority · field source_edge_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3a92e3ebef75"></a>definition TargetProgress · field phase | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-7c59ae0b6897"></a>definition TargetProgress · field unit · anyOf alternative 1 | `length · characters · contract_max` | maximum=40; minimum=1; reason="schema-maximum" |
| <a id="s-6abdaeda7977"></a>definition TargetSemanticConformance · field conformance_vectors_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-143df259a185"></a>definition TargetSemanticConformance · field profile_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-b92ace78d4e7"></a>definition TransformPlan · field observation_result_sha256s · items | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-53d17c8c75a5"></a>definition TransformPlan · field operation_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-d9e38e912f3d"></a>definition TransformPlan · field plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-1ea60139c84b"></a>definition TransformPlan · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-3498a29c054c"></a>definition WorkIdentity · field work_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-801d28c10d14"></a>definition WorkflowPlan · field target_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-aef415faeaf9"></a>definition WorkflowPlan · field workflow_plan_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-341421cbfb4a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-7f37c6db4fd4"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-3211c39da022"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-target](../../../evidence/sources.md#src-2c42f9d39a0b) — `reference/stove0/packages/target-support/src/stove0_target_support/schemas.py::target_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-target/schemas/TargetConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dacd9898c46672af35bfeb83fabcf79bcb811edbef134d19b75e3c4a7f66b543 -->

```json
{
  "$defs": {
    "AcceptedTargetJob": {
      "additionalProperties": false,
      "description": "Durable, non-secret identity of one accepted target job request.",
      "properties": {
        "declaration": {
          "$ref": "#/$defs/TargetJobDeclaration"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        }
      },
      "required": [
        "declaration",
        "request_sha256"
      ],
      "title": "AcceptedTargetJob",
      "type": "object"
    },
    "ArtifactDispositionSetIdentity": {
      "description": "Small identity for one sealed claim-scoped relational disposition set.",
      "properties": {
        "disposition_count": {
          "title": "Disposition Count",
          "type": "integer"
        },
        "output_artifact_count": {
          "title": "Output Artifact Count",
          "type": "integer"
        },
        "output_edge_count": {
          "title": "Output Edge Count",
          "type": "integer"
        },
        "sha256": {
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "disposition_count",
        "output_edge_count",
        "output_artifact_count",
        "sha256"
      ],
      "title": "ArtifactDispositionSetIdentity",
      "type": "object"
    },
    "ArtifactSelectionRef": {
      "additionalProperties": false,
      "description": "Closed reference to a separately retained selection document.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Selection Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "selection_sha256",
        "artifact_count",
        "total_bytes"
      ],
      "title": "ArtifactSelectionRef",
      "type": "object"
    },
    "ArtifactSubject": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "role",
        "collection",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "ArtifactSubject",
      "type": "object"
    },
    "BranchWorkBinding": {
      "additionalProperties": false,
      "description": "Stable parent/branch lineage for one ordinary child work identity.",
      "properties": {
        "artifact_selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Artifact Selection Sha256",
          "type": "string"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
          "type": "string"
        },
        "decision_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Decision Sha256",
          "type": "string"
        },
        "kind": {
          "const": "branch",
          "default": "branch",
          "title": "Kind",
          "type": "string"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Parent Work Id",
          "type": "string"
        }
      },
      "required": [
        "parent_work_id",
        "branch_id",
        "decision_sha256",
        "artifact_selection_sha256"
      ],
      "title": "BranchWorkBinding",
      "type": "object"
    },
    "CollectionId": {
      "minimum": 1,
      "type": "integer"
    },
    "CollectionRootRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootRef",
      "type": "object"
    },
    "ControllerEvidence": {
      "additionalProperties": false,
      "properties": {
        "controller_evidence_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Controller Evidence Sha256",
          "type": "string"
        },
        "execution_envelope": {
          "$ref": "#/$defs/ExecutionEnvelope"
        },
        "format": {
          "const": "stove0-controller-evidence/v1",
          "default": "stove0-controller-evidence/v1",
          "title": "Format",
          "type": "string"
        }
      },
      "required": [
        "execution_envelope",
        "controller_evidence_sha256"
      ],
      "title": "ControllerEvidence",
      "type": "object"
    },
    "EffectPlan": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-effect-target/v1",
          "default": "stove0-effect-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_contract_sha256",
        "plan_sha256"
      ],
      "title": "EffectPlan",
      "type": "object"
    },
    "EvaluationBinding": {
      "additionalProperties": false,
      "description": "Immutable membership of one work item in a trial/evaluation matrix.",
      "properties": {
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Evaluation Id",
          "type": "string"
        },
        "matrix_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Matrix Sha256",
          "type": "string"
        },
        "parameters": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Parameters",
          "type": "object"
        },
        "variant_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Variant Id",
          "type": "string"
        }
      },
      "required": [
        "evaluation_id",
        "matrix_sha256",
        "variant_id"
      ],
      "title": "EvaluationBinding",
      "type": "object"
    },
    "ExecutionEnvelope": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
          "type": "string"
        },
        "execution_envelope_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Envelope Sha256",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "format": {
          "const": "stove0-execution-envelope/v1",
          "default": "stove0-execution-envelope/v1",
          "title": "Format",
          "type": "string"
        },
        "target_plan": {
          "$ref": "#/$defs/TargetPlanBinding"
        },
        "workflow_plan": {
          "$ref": "#/$defs/WorkflowPlan"
        }
      },
      "required": [
        "claim_id",
        "fence",
        "workflow_plan",
        "target_plan",
        "execution_envelope_sha256"
      ],
      "title": "ExecutionEnvelope",
      "type": "object"
    },
    "ExternalEffectReceipt": {
      "additionalProperties": false,
      "properties": {
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-external-effect-receipt/v1",
          "default": "stove0-external-effect-receipt/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Receipt Sha256",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "result": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Result",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-external-effect-receipt"
          }
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "request_sha256",
        "target_contract_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256",
        "result",
        "receipt_sha256"
      ],
      "title": "ExternalEffectReceipt",
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
    "JoinWorkBinding": {
      "additionalProperties": false,
      "description": "Stable branch-set lineage for one ordinary join work identity.",
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Branch Set Sha256",
          "type": "string"
        },
        "kind": {
          "const": "join",
          "default": "join",
          "title": "Kind",
          "type": "string"
        },
        "members": {
          "items": {
            "$ref": "#/$defs/JoinWorkMemberBinding"
          },
          "minItems": 2,
          "title": "Members",
          "type": "array"
        },
        "parent_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Parent Work Id",
          "type": "string"
        }
      },
      "required": [
        "parent_work_id",
        "branch_set_sha256",
        "members"
      ],
      "title": "JoinWorkBinding",
      "type": "object"
    },
    "JoinWorkMemberBinding": {
      "additionalProperties": false,
      "description": "Exact successful branch result used to derive one join work identity.",
      "properties": {
        "artifact_selection_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Artifact Selection Sha256",
          "type": "string"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
          "type": "string"
        },
        "producer_settlement_sha256": {
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
          "title": "Producer Settlement Sha256"
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Settlement Sha256",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "settlement_sha256",
        "artifact_selection_sha256"
      ],
      "title": "JoinWorkMemberBinding",
      "type": "object"
    },
    "JsonSchemaDocument": {
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
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
      "type": "object"
    },
    "JsonValue": {},
    "ObservationEvidence": {
      "additionalProperties": false,
      "description": "Complete routing evidence: immutable request plus accepted result.",
      "properties": {
        "request": {
          "$ref": "#/$defs/ObservationRequest"
        },
        "result": {
          "$ref": "#/$defs/ObservationResult"
        }
      },
      "required": [
        "request",
        "result"
      ],
      "title": "ObservationEvidence",
      "type": "object"
    },
    "ObservationFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "ObservationFailure",
      "type": "object"
    },
    "ObservationInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "ObservationInapplicable",
      "type": "object"
    },
    "ObservationRequest": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "title": "Format",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Descriptor Sha256",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Observer Registration Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
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
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "observer_registration_id",
        "observer_descriptor_sha256",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "request_id"
      ],
      "title": "ObservationRequest",
      "type": "object"
    },
    "ObservationResult": {
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Execution Evidence",
          "type": "object"
        },
        "facts": {
          "anyOf": [
            {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Facts"
        },
        "facts_schema": {
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
        "facts_sha256": {
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
          "title": "Facts Sha256"
        },
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObservationFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "stove0-observation-result/v1",
          "default": "stove0-observation-result/v1",
          "title": "Format",
          "type": "string"
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObservationInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "observer": {
          "$ref": "#/$defs/ObserverImplementation"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "result_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Result Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "observed",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        }
      },
      "required": [
        "request_id",
        "state",
        "observer",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "result_sha256"
      ],
      "title": "ObservationResult",
      "type": "object"
    },
    "ObserverImplementation": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Version",
          "type": "string"
        }
      },
      "required": [
        "id",
        "version",
        "source_revision",
        "descriptor_sha256"
      ],
      "title": "ObserverImplementation",
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
        "source_retirement_permitted": {
          "default": false,
          "title": "Source Retirement Permitted",
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
    "OperationRef": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256"
      ],
      "title": "OperationRef",
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
    "OutputArtifactRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
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
        "count"
      ],
      "title": "OutputArtifactRoleCount",
      "type": "object"
    },
    "OutputArtifactSetIdentity": {
      "additionalProperties": false,
      "description": "Small identity for target outputs already registered with Riverhog.",
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "title": "Artifact Count",
          "type": "integer"
        },
        "roles": {
          "items": {
            "$ref": "#/$defs/OutputArtifactRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "title": "Total Bytes",
          "type": "integer"
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "roles",
        "sha256"
      ],
      "title": "OutputArtifactSetIdentity",
      "type": "object"
    },
    "OutputCollectionRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        },
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Derivation Sha256",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity",
        "derivation_sha256"
      ],
      "title": "OutputCollectionRef",
      "type": "object"
    },
    "RecipeRef": {
      "additionalProperties": false,
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
      "title": "RecipeRef",
      "type": "object"
    },
    "SemanticIntentConformanceVector": {
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "title": "Accepted",
          "type": "boolean"
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
        }
      },
      "required": [
        "id",
        "accepted",
        "intent"
      ],
      "title": "SemanticIntentConformanceVector",
      "type": "object"
    },
    "SemanticIntentConformanceVectors": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-semantic-intent-conformance/v1",
          "default": "stove0-semantic-intent-conformance/v1",
          "title": "Format",
          "type": "string"
        },
        "profile_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Profile Id",
          "type": "string"
        },
        "vectors": {
          "items": {
            "$ref": "#/$defs/SemanticIntentConformanceVector"
          },
          "minItems": 2,
          "title": "Vectors",
          "type": "array"
        }
      },
      "required": [
        "profile_id",
        "vectors"
      ],
      "title": "SemanticIntentConformanceVectors",
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
    "TargetConformanceCoverage": {
      "additionalProperties": false,
      "properties": {
        "advertised": {
          "minimum": 0,
          "title": "Advertised",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "exercised": {
          "minimum": 0,
          "title": "Exercised",
          "type": "integer"
        }
      },
      "required": [
        "advertised",
        "exercised",
        "complete"
      ],
      "title": "TargetConformanceCoverage",
      "type": "object"
    },
    "TargetContract": {
      "additionalProperties": false,
      "properties": {
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/TargetOperationSupport"
          },
          "minItems": 1,
          "title": "Operations",
          "type": "array"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "title": "Transport",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_digest",
        "operations",
        "contract_sha256"
      ],
      "title": "TargetContract",
      "type": "object"
    },
    "TargetExecutionEvidence": {
      "additionalProperties": false,
      "properties": {
        "execution_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Execution Sha256",
          "type": "string"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "runtime": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Runtime",
          "type": "object"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        }
      },
      "required": [
        "target_contract_sha256",
        "operation_contract_sha256",
        "plan_sha256",
        "execution_sha256"
      ],
      "title": "TargetExecutionEvidence",
      "type": "object"
    },
    "TargetFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "TargetFailure",
      "type": "object"
    },
    "TargetInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "TargetInapplicable",
      "type": "object"
    },
    "TargetInputAuthority": {
      "additionalProperties": false,
      "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
      "properties": {
        "roles": {
          "items": {
            "$ref": "#/$defs/TargetInputRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        }
      },
      "required": [
        "selection",
        "roles"
      ],
      "title": "TargetInputAuthority",
      "type": "object"
    },
    "TargetInputRoleCount": {
      "additionalProperties": false,
      "properties": {
        "count": {
          "minimum": 1,
          "title": "Count",
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
        "count"
      ],
      "title": "TargetInputRoleCount",
      "type": "object"
    },
    "TargetJobDeclaration": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
          "type": "string"
        },
        "controller_evidence": {
          "$ref": "#/$defs/ControllerEvidence"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "plan": {
          "discriminator": {
            "mapping": {
              "stove0-effect-target/v1": "#/$defs/EffectPlan",
              "stove0-transform-target/v1": "#/$defs/TransformPlan"
            },
            "propertyName": "protocol"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/TransformPlan"
            },
            {
              "$ref": "#/$defs/EffectPlan"
            }
          ],
          "title": "Plan"
        },
        "workspace_assurance": {
          "enum": [
            "encrypted",
            "ephemeral"
          ],
          "title": "Workspace Assurance",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "claim_id",
        "fence",
        "controller_evidence",
        "plan",
        "workspace_assurance"
      ],
      "title": "TargetJobDeclaration",
      "type": "object"
    },
    "TargetJobStatus": {
      "additionalProperties": false,
      "allOf": [
        {
          "else": {
            "properties": {
              "failure": {
                "type": "null"
              }
            }
          },
          "if": {
            "properties": {
              "state": {
                "const": "failed"
              }
            }
          },
          "then": {
            "properties": {
              "failure": {
                "type": "object"
              }
            },
            "required": [
              "failure"
            ]
          }
        }
      ],
      "properties": {
        "attempt": {
          "minimum": 1,
          "title": "Attempt",
          "type": "integer"
        },
        "derivation": {
          "anyOf": [
            {
              "additionalProperties": true,
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Derivation"
        },
        "effect_receipt": {
          "anyOf": [
            {
              "$ref": "#/$defs/ExternalEffectReceipt"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution_evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetExecutionEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "output_collection": {
          "anyOf": [
            {
              "$ref": "#/$defs/OutputCollectionRef"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetProductionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "progress": {
          "$ref": "#/$defs/TargetProgress"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "request_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "queued",
            "running",
            "canceling",
            "interrupted",
            "inapplicable",
            "succeeded",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "state",
        "attempt",
        "request_sha256",
        "plan_sha256",
        "progress"
      ],
      "title": "TargetJobStatus",
      "type": "object"
    },
    "TargetOperationConformance": {
      "additionalProperties": false,
      "properties": {
        "intent_semantics_conformance_vectors_sha256": {
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
          "title": "Intent Semantics Conformance Vectors Sha256"
        },
        "intent_semantics_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Intent Semantics Id"
        },
        "intent_semantics_sha256": {
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
          "title": "Intent Semantics Sha256"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Options Schema Sha256",
          "type": "string"
        },
        "result_kind": {
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        },
        "semantic_conformance": {
          "enum": [
            "not-exercised",
            "schema-only",
            "exercised"
          ],
          "title": "Semantic Conformance",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "result_kind",
        "options_schema_sha256",
        "semantic_conformance"
      ],
      "title": "TargetOperationConformance",
      "type": "object"
    },
    "TargetOperationConformanceEvidence": {
      "additionalProperties": false,
      "properties": {
        "accepted_job": {
          "$ref": "#/$defs/AcceptedTargetJob"
        },
        "job_status": {
          "$ref": "#/$defs/TargetJobStatus"
        },
        "operation": {
          "$ref": "#/$defs/OperationContract"
        },
        "operation_id": {
          "title": "Operation Id",
          "type": "string"
        },
        "preflight": {
          "$ref": "#/$defs/TargetPreflightResponse"
        },
        "preflight_request": {
          "$ref": "#/$defs/TargetPreflightRequest"
        },
        "semantic_conformance": {
          "$ref": "#/$defs/TargetSemanticConformance"
        },
        "submission": {
          "$ref": "#/$defs/TargetJobStatus"
        }
      },
      "required": [
        "operation_id",
        "operation",
        "semantic_conformance",
        "preflight_request",
        "preflight",
        "accepted_job",
        "submission",
        "job_status"
      ],
      "title": "TargetOperationConformanceEvidence",
      "type": "object"
    },
    "TargetOperationSupport": {
      "additionalProperties": false,
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "title": "Result Kind",
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "options_schema"
      ],
      "title": "TargetOperationSupport",
      "type": "object"
    },
    "TargetPlanBinding": {
      "additionalProperties": false,
      "description": "Opaque binding to a target-owned preflight plan.\n\nThe target protocol owns the plan schema and canonicalization algorithm. stove0\nretains the complete validated plan document and its target-issued digest, but\ndeliberately does not reinterpret or re-hash the plan with stove0's canonical\nJSON rules. This prevents two authorities from disagreeing about target plan\nidentity while preserving the full document in the execution envelope.",
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "plan": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Plan",
          "type": "object"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Protocol",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        }
      },
      "required": [
        "protocol",
        "target_implementation_id",
        "target_contract_sha256",
        "operation_contract_sha256",
        "plan",
        "plan_sha256"
      ],
      "title": "TargetPlanBinding",
      "type": "object"
    },
    "TargetPreflightRequest": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ObservationEvidence"
          },
          "title": "Observations",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "title": "Protocol",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent"
      ],
      "title": "TargetPreflightRequest",
      "type": "object"
    },
    "TargetPreflightResponse": {
      "additionalProperties": false,
      "properties": {
        "plan": {
          "discriminator": {
            "mapping": {
              "stove0-effect-target/v1": "#/$defs/EffectPlan",
              "stove0-transform-target/v1": "#/$defs/TransformPlan"
            },
            "propertyName": "protocol"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/TransformPlan"
            },
            {
              "$ref": "#/$defs/EffectPlan"
            }
          ],
          "title": "Plan"
        },
        "target": {
          "$ref": "#/$defs/TargetContract"
        }
      },
      "required": [
        "target",
        "plan"
      ],
      "title": "TargetPreflightResponse",
      "type": "object"
    },
    "TargetProductionAuthority": {
      "additionalProperties": false,
      "properties": {
        "disposition_count": {
          "minimum": 1,
          "title": "Disposition Count",
          "type": "integer"
        },
        "disposition_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Disposition Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-target-production/v1",
          "default": "stove0-target-production/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "outputs": {
          "$ref": "#/$defs/OutputArtifactSetIdentity"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Production Sha256",
          "type": "string"
        },
        "riverhog_disposition_set": {
          "$ref": "#/$defs/ArtifactDispositionSetIdentity"
        },
        "source_edge_count": {
          "minimum": 1,
          "title": "Source Edge Count",
          "type": "integer"
        },
        "source_edge_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Edge Sha256",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "plan_sha256",
        "outputs",
        "disposition_count",
        "disposition_sha256",
        "source_edge_count",
        "source_edge_sha256",
        "riverhog_disposition_set",
        "production_sha256"
      ],
      "title": "TargetProductionAuthority",
      "type": "object"
    },
    "TargetProgress": {
      "additionalProperties": false,
      "properties": {
        "completed": {
          "minimum": 0,
          "title": "Completed",
          "type": "integer"
        },
        "phase": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Phase",
          "type": "string"
        },
        "total": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Total"
        },
        "unit": {
          "anyOf": [
            {
              "maxLength": 40,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Unit"
        }
      },
      "required": [
        "phase",
        "completed"
      ],
      "title": "TargetProgress",
      "type": "object"
    },
    "TargetSemanticConformance": {
      "additionalProperties": false,
      "properties": {
        "accepted_vector_ids": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Accepted Vector Ids",
          "type": "array"
        },
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
        "profile_id": {
          "title": "Profile Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rejected_vector_ids": {
          "default": [],
          "items": {
            "type": "string"
          },
          "title": "Rejected Vector Ids",
          "type": "array"
        },
        "status": {
          "enum": [
            "schema-only",
            "exercised"
          ],
          "title": "Status",
          "type": "string"
        },
        "vectors": {
          "anyOf": [
            {
              "$ref": "#/$defs/SemanticIntentConformanceVectors"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "profile_id",
        "profile_sha256",
        "status"
      ],
      "title": "TargetSemanticConformance",
      "type": "object"
    },
    "TransformPlan": {
      "additionalProperties": false,
      "properties": {
        "inputs": {
          "$ref": "#/$defs/TargetInputAuthority"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Intent",
          "type": "object"
        },
        "observation_result_sha256s": {
          "default": [],
          "items": {
            "pattern": "^[0-9a-f]{64}$",
            "type": "string"
          },
          "title": "Observation Result Sha256S",
          "type": "array"
        },
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Operation Contract Sha256",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Operation Id",
          "type": "string"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-transform-target/v1",
          "default": "stove0-transform-target/v1",
          "title": "Protocol",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Target Implementation Id",
          "type": "string"
        },
        "target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Target Options",
          "type": "object"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "inputs",
        "intent",
        "target_implementation_id",
        "target_contract_sha256",
        "plan_sha256"
      ],
      "title": "TransformPlan",
      "type": "object"
    },
    "WorkIdentity": {
      "additionalProperties": false,
      "properties": {
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Effective Intent",
          "type": "object"
        },
        "evaluation": {
          "anyOf": [
            {
              "$ref": "#/$defs/EvaluationBinding"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "fork_join": {
          "anyOf": [
            {
              "discriminator": {
                "mapping": {
                  "branch": "#/$defs/BranchWorkBinding",
                  "join": "#/$defs/JoinWorkBinding"
                },
                "propertyName": "kind"
              },
              "oneOf": [
                {
                  "$ref": "#/$defs/BranchWorkBinding"
                },
                {
                  "$ref": "#/$defs/JoinWorkBinding"
                }
              ]
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Fork Join"
        },
        "format": {
          "const": "stove0-work/v1",
          "default": "stove0-work/v1",
          "title": "Format",
          "type": "string"
        },
        "inputs": {
          "items": {
            "$ref": "#/$defs/CollectionRootRef"
          },
          "minItems": 1,
          "title": "Inputs",
          "type": "array"
        },
        "recipe": {
          "$ref": "#/$defs/RecipeRef"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "recipe",
        "inputs",
        "work_id"
      ],
      "title": "WorkIdentity",
      "type": "object"
    },
    "WorkflowPlan": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-workflow-plan/v1",
          "default": "stove0-workflow-plan/v1",
          "title": "Format",
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
        "observations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ObservationEvidence"
          },
          "title": "Observations",
          "type": "array"
        },
        "operation": {
          "$ref": "#/$defs/OperationRef"
        },
        "output_policy": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Output Policy",
          "type": "object"
        },
        "requested_target_options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Requested Target Options",
          "type": "object"
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
        "retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "title": "Retirement Grace Seconds",
          "type": "integer"
        },
        "retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "title": "Retirement Policy",
          "type": "string"
        },
        "target_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Contract Sha256",
          "type": "string"
        },
        "target_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Target Registration Id",
          "type": "string"
        },
        "work": {
          "$ref": "#/$defs/WorkIdentity"
        },
        "workflow_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Workflow Plan Sha256",
          "type": "string"
        }
      },
      "required": [
        "work",
        "operation",
        "target_registration_id",
        "target_contract_sha256",
        "workflow_plan_sha256"
      ],
      "title": "WorkflowPlan",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "coverage": {
      "$ref": "#/$defs/TargetConformanceCoverage"
    },
    "format": {
      "const": "stove0-target-conformance-result/v1",
      "default": "stove0-target-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "operation_evidence": {
      "default": [],
      "items": {
        "$ref": "#/$defs/TargetOperationConformanceEvidence"
      },
      "title": "Operation Evidence",
      "type": "array"
    },
    "operations": {
      "items": {
        "$ref": "#/$defs/TargetOperationConformance"
      },
      "title": "Operations",
      "type": "array"
    },
    "status": {
      "enum": [
        "conformant",
        "partially-exercised",
        "inspected"
      ],
      "title": "Status",
      "type": "string"
    },
    "target": {
      "$ref": "#/$defs/TargetContract"
    }
  },
  "required": [
    "status",
    "target",
    "coverage",
    "operations"
  ],
  "title": "TargetConformanceResult",
  "type": "object"
}
```
