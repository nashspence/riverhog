# schemas: CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadprovenancejournal-ba2d3d9a98:4790d84516 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadProvenanceJournalStatusDocument`

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
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: CollectionUploadProvenanceJournalStatusDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `accepted_bytes` | yes | integer |  |
| `bytes` | yes | integer |  |
| `current_bytes` | no | object (2 fields) |  |
| `current_path` | no | object (2 fields) |  |
| `current_sha256` | no | object (2 fields) |  |
| `current_state_id` | no | object (1 fields) |  |
| `failure` | no | object (2 fields) |  |
| `journal_id` | yes | #/components/schemas/ProvenanceJournalId |  |
| `sha256` | yes | string |  |
| `state` | yes | string |  |
