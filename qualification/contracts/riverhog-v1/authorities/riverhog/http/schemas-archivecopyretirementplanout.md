# schemas: ArchiveCopyRetirementPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementplanout:3f3e4bb826 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementPlanOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `contract_max` | maximum=0, reason=state-conditioned-empty-set |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: ArchiveCopyRetirementPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `billing_note` | yes | string |  |
| `blockers` | yes | array |  |
| `challenge` | yes | object (2 fields) |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `expires_at` | yes | string |  |
| `retained_copies` | yes | array |  |
| `retired_retrieval_job_count` | yes | integer |  |
| `status` | yes | string |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |
| `target_copy` | yes | #/components/schemas/ArchiveCopyRetirementTargetOut |  |
| `verification_note` | yes | string |  |
| `warning` | yes | string |  |
