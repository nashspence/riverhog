# schemas: CollectionDeletionPlanOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionplanout:21e8fd2ce3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionPlanOut`

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
| cardinality | items | `contract_max` | maximum=55, reason=bounded-diagnostic-sample-with-explicit-overflow-markers |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract

- `title`: CollectionDeletionPlanOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_copies` | yes | array |  |
| `archive_object_count` | yes | integer |  |
| `billing_note` | yes | string |  |
| `blockers` | yes | array |  |
| `bytes` | yes | integer |  |
| `challenge` | yes | object (2 fields) |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `expires_at` | yes | string |  |
| `file_count` | yes | integer |  |
| `inventory_identity` | yes | string |  |
| `metadata_rows` | yes | object |  |
| `remote_storage_bytes` | yes | integer |  |
| `retirement_claim` | no | object (1 fields) |  |
| `status` | yes | string |  |
| `upload_file_count` | yes | integer |  |
| `warning` | yes | string |  |
