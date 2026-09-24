# stove0: HTTP Operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Policies](../../../policies/index.md)

Callable HTTP operations.

## Contract elements

**📦** Some guarantees for work spanning pages or chunks still lack supporting evidence. Follow the package marker for the affected guarantees and contracts. This records an evidence gap, not an observed bug; unmarked entries imply no approval.

- [GET /health/live](get-health-live.md)
- [GET /health/ready](get-health-ready.md)
- [GET /v1/admin/scheduler](get-v1-admin-scheduler.md)
- [GET /v1/admission-policies](get-v1-admission-policies.md)
- **[GET /v1/admissions](get-v1-admissions.md)** [📦](get-v1-admissions.md#evidence-gaps)
- [GET /v1/admissions/{admission_id}](get-v1-admissions-admission-id.md)
- **[GET /v1/artifact-selections/{selection_sha256}](get-v1-artifact-selections-selection-sha256.md)** [📦](get-v1-artifact-selections-selection-sha256.md#evidence-gaps)
- **[GET /v1/departure-effects](get-v1-departure-effects.md)** [📦](get-v1-departure-effects.md#evidence-gaps)
- [GET /v1/departure-effects/{departure_id}](get-v1-departure-effects-departure-id.md)
- [GET /v1/departure-policies](get-v1-departure-policies.md)
- **[GET /v1/evaluations](get-v1-evaluations.md)** [📦](get-v1-evaluations.md#evidence-gaps)
- [GET /v1/evaluations/{evaluation_id}](get-v1-evaluations-evaluation-id.md)
- **[GET /v1/events](get-v1-events.md)** [📦](get-v1-events.md#evidence-gaps)
- [GET /v1/recipes](get-v1-recipes.md)
- [GET /v1/recipes/{recipe_id}](get-v1-recipes-recipe-id.md)
- **[GET /v1/target-executions/{job_id}/inputs](get-v1-target-executions-job-id-inputs.md)** [📦](get-v1-target-executions-job-id-inputs.md#evidence-gaps)
- **[GET /v1/work](get-v1-work.md)** [📦](get-v1-work.md#evidence-gaps)
- [GET /v1/work/{work_id}](get-v1-work-work-id.md)
- [GET /v1/work/{work_id}/coordination](get-v1-work-work-id-coordination.md)
- [POST /v1/admin/scheduler/run](post-v1-admin-scheduler-run.md)
- [POST /v1/admission-policies/{policy_id}:backfill](post-v1-admission-policies-policy-id-backfill.md)
- [POST /v1/admission-policies/{policy_id}:rebaseline](post-v1-admission-policies-policy-id-rebaseline.md)
- [POST /v1/departure-policies/{policy_id}:rebaseline](post-v1-departure-policies-policy-id-rebaseline.md)
- [POST /v1/evaluations](post-v1-evaluations.md)
- [POST /v1/evaluations/{evaluation_id}/cancel](post-v1-evaluations-evaluation-id-cancel.md)
- [POST /v1/evaluations/{evaluation_id}/step](post-v1-evaluations-evaluation-id-step.md)
- [POST /v1/evaluations/{evaluation_id}/variants/{variant_id}/retry](post-v1-evaluations-evaluation-id-variants-variant-id-retry.md)
- [POST /v1/target-executions/{job_id}/production/seal](post-v1-target-executions-job-id-production-seal.md)
- [POST /v1/work](post-v1-work.md)
- [POST /v1/work/{work_id}/cancel](post-v1-work-work-id-cancel.md)
- [POST /v1/work/{work_id}/retry](post-v1-work-work-id-retry.md)
- [POST /v1/work/{work_id}/step](post-v1-work-work-id-step.md)
- [POST /v1/workflow-previews](post-v1-workflow-previews.md)
- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [PUT /v1/target-executions/{job_id}/dispositions/{input_id}](put-v1-target-executions-job-id-dispositions-input-id.md)
- [PUT /v1/target-executions/{job_id}/outputs/{artifact_id}](put-v1-target-executions-job-id-outputs-artifact-id.md)
- [PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}](put-v1-target-executions-job-id-source-edges-output-id-input-id.md)
