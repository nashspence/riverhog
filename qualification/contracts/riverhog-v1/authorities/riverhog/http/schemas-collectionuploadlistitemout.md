# schemas: CollectionUploadListItemOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadlistitemout:93ae8d69d7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadListItemOut`

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
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=0, reason=schema-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CollectionUploadListItemOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_store` | yes | #/components/schemas/ArchiveStoreName |  |
| `bytes` | yes | integer |  |
| `collection_id` | yes | #/components/schemas/CollectionId |  |
| `created_at` | yes | object (2 fields) |  |
| `custody` | yes | object (3 fields) |  |
| `custody_mode` | yes | string |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | object (2 fields) |  |
| `description_publication` | yes | string |  |
| `description_revision` | yes | object (2 fields) |  |
| `encryption_format` | yes | string |  |
| `files` | yes | integer |  |
| `ingest_source` | yes | object (2 fields) |  |
| `orphaned_at` | yes | object (2 fields) |  |
| `passphrase_id` | yes | string |  |
| `state` | yes | string |  |
| `tag_count` | yes | integer |  |
| `tag_publication` | yes | string |  |
| `tag_revision` | no | object (2 fields) |  |
| `tag_set_identity` | no | object (2 fields) |  |
| `upload_state_expires_at` | yes | object (2 fields) |  |
