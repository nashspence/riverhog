# schemas: CollectionSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionsummaryout:7097828626 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionSummaryOut`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=0, reason=schema-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=9007199254740991, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CollectionSummaryOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_copy_count` | yes | integer |  |
| `archive_root_sha256` | yes | string |  |
| `bytes` | yes | integer |  |
| `content_identity` | yes | string |  |
| `created_at` | yes | string |  |
| `description` | yes | object (1 fields) |  |
| `description_identity` | yes | string |  |
| `description_publication` | yes | string |  |
| `description_revision` | yes | integer |  |
| `encryption_format` | yes | string |  |
| `files` | yes | integer |  |
| `id` | yes | #/components/schemas/CollectionId |  |
| `passphrase_id` | yes | string |  |
| `remote_storage_bytes` | yes | integer |  |
| `tag_publication` | yes | string |  |
| `tag_revision` | yes | integer |  |
| `tag_set_identity` | yes | string |  |
