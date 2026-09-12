# schemas: CreateOrResumeCollectionUploadSessionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createorresumecollectionuploadsessionrequest:b45d70029f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionRequest`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/bounded-segment/v1`
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
| encoded-size | bytes | `contract_max` | maximum=4096, reason=bounded-lifecycle-event-context |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=200, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=100, minimum=None, reason=bounded-upload-staging-step; collection-tag-set-is-unbounded |

## Contract

- `title`: CreateOrResumeCollectionUploadSessionRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_store` | no | object (1 fields) |  |
| `custody_mode` | no | string |  |
| `description` | no | object (1 fields) |  |
| `event_context` | no | object (2 fields) |  |
| `idempotency_key` | yes | string |  |
| `ingest_source` | no | object (2 fields) |  |
| `initial_tag_set_identity` | yes | string |  |
| `provenance_mode` | no | string |  |
| `provenance_omission_reason` | no | object (2 fields) |  |
| `tags` | no | array |  |
