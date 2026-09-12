# schemas: CreateOrResumeCollectionUploadSessionOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-createorresumecollectionuploadsessionout:7e70bea877 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 9 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CreateOrResumeCollectionUploadSessionOut`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=0, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CreateOrResumeCollectionUploadSessionOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_next_attempt_at` | yes | object (2 fields) |  |
| `archive_phase` | yes | string |  |
| `archive_phase_updated_at` | yes | string |  |
| `archive_root_sha256` | no | object (2 fields) |  |
| `archive_storage_prefix` | no | object (2 fields) |  |
| `archive_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `archive_total_bytes` | no | object (2 fields) |  |
| `archive_total_units` | no | object (2 fields) |  |
| `archive_uploaded_bytes` | no | object (2 fields) |  |
| `archive_uploaded_units` | no | object (2 fields) |  |
| `bytes_total` | yes | integer |  |
| `collection` | yes | object (1 fields) |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `content_identity` | no | object (2 fields) |  |
| `created_at` | yes | string |  |
| `custody` | yes | object (3 fields) |  |
| `custody_mode` | yes | string |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | object (2 fields) |  |
| `description_publication` | yes | string |  |
| `description_revision` | yes | object (2 fields) |  |
| `encryption_format` | yes | string |  |
| `files_total` | yes | integer |  |
| `ingest_source` | yes | object (2 fields) |  |
| `latest_failure` | yes | object (2 fields) |  |
| `orphaned_at` | yes | object (2 fields) |  |
| `passphrase_id` | yes | string |  |
| `provenance_identity` | no | object (2 fields) |  |
| `provenance_mode` | yes | string |  |
| `registration_constraints` | yes | object (1 fields) |  |
| `resumed` | yes | boolean |  |
| `state` | yes | string |  |
| `tag_count` | yes | integer |  |
| `tag_publication` | yes | string |  |
| `tag_revision` | no | object (2 fields) |  |
| `tag_set_identity` | no | object (2 fields) |  |
| `upload_state_expires_at` | yes | object (2 fields) |  |
