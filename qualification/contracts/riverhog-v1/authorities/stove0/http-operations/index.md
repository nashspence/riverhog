# stove0: HTTP Operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Policies](../../../policies/index.md)

Callable HTTP operations.

Contract elements: **33** · Extent decisions: **10**

| Policy | Count |
|---|---:|
| [compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0) | 33 |
| [extent-rule/route-progression/v1](../../../policies/index.md#p-6b76b527cb) | 6 |
| [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0) | 4 |

## Semantic dossiers

| Dossier | Extent decisions |
|---|---:|
| [GET /health/live](get-health-live.md) | 0 |
| [GET /health/ready](get-health-ready.md) | 0 |
| [GET /v1/admin/scheduler](get-v1-admin-scheduler.md) | 0 |
| [GET /v1/admission-policies](get-v1-admission-policies.md) | 0 |
| [GET /v1/admissions](get-v1-admissions.md) | 2 |
| [GET /v1/admissions/{admission_id}](get-v1-admissions-admission-id.md) | 0 |
| [GET /v1/artifact-selections/{selection_sha256}](get-v1-artifact-selections-selection-sha256.md) | 1 |
| [GET /v1/evaluations](get-v1-evaluations.md) | 2 |
| [GET /v1/evaluations/{evaluation_id}](get-v1-evaluations-evaluation-id.md) | 0 |
| [GET /v1/events](get-v1-events.md) | 2 |
| [GET /v1/recipes](get-v1-recipes.md) | 0 |
| [GET /v1/recipes/{recipe_id}](get-v1-recipes-recipe-id.md) | 0 |
| [GET /v1/target-executions/{job_id}/inputs](get-v1-target-executions-job-id-inputs.md) | 1 |
| [GET /v1/work](get-v1-work.md) | 2 |
| [GET /v1/work/{work_id}](get-v1-work-work-id.md) | 0 |
| [GET /v1/work/{work_id}/coordination](get-v1-work-work-id-coordination.md) | 0 |
| [POST /v1/admin/scheduler/run](post-v1-admin-scheduler-run.md) | 0 |
| [POST /v1/admission-policies/{policy_id}:backfill](post-v1-admission-policies-policy-id-backfill.md) | 0 |
| [POST /v1/admission-policies/{policy_id}:rebaseline](post-v1-admission-policies-policy-id-rebaseline.md) | 0 |
| [POST /v1/evaluations](post-v1-evaluations.md) | 0 |
| [POST /v1/evaluations/{evaluation_id}/cancel](post-v1-evaluations-evaluation-id-cancel.md) | 0 |
| [POST /v1/evaluations/{evaluation_id}/step](post-v1-evaluations-evaluation-id-step.md) | 0 |
| [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](post-v1-evaluations-evaluation-id-variants-variant-id-retry.md) | 0 |
| [POST /v1/target-executions/{job_id}/production/seal](post-v1-target-executions-job-id-production-seal.md) | 0 |
| [POST /v1/work](post-v1-work.md) | 0 |
| [POST /v1/work/{work_id}/cancel](post-v1-work-work-id-cancel.md) | 0 |
| [POST /v1/work/{work_id}/retry](post-v1-work-work-id-retry.md) | 0 |
| [POST /v1/work/{work_id}/step](post-v1-work-work-id-step.md) | 0 |
| [POST /v1/workflow-previews](post-v1-workflow-previews.md) | 0 |
| [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](put-v1-evaluations-evaluation-id-variants-variant-id-review.md) | 0 |
| [PUT /v1/target-executions/{job_id}/dispositions/{input_id}](put-v1-target-executions-job-id-dispositions-input-id.md) | 0 |
| [PUT /v1/target-executions/{job_id}/outputs/{artifact_id}](put-v1-target-executions-job-id-outputs-artifact-id.md) | 0 |
| [PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}](put-v1-target-executions-job-id-source-edges-output-id-input-id.md) | 0 |
