# schemas: ProcessingClaimDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimdocument:a0c4a1f332 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 13 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimDocument`

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
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=1000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=4194304, reason=bounded-work-document-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ProcessingClaimDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `abandoned_at` | no | object (2 fields) |  |
| `abandonment_reason` | no | object (2 fields) |  |
| `consumer` | yes | #/components/schemas/ProcessingClaimConsumerDocument |  |
| `created_at` | yes | string |  |
| `expires_at` | yes | string |  |
| `fence` | yes | integer |  |
| `format` | yes | string |  |
| `id` | yes | string |  |
| `inputs` | yes | #/components/schemas/ReceivingSetDocument |  |
| `outcome_settlement` | no | object (1 fields) |  |
| `outcomes` | yes | #/components/schemas/OutcomeSetDocument |  |
| `output_collection_id` | no | object (1 fields) |  |
| `plan` | no | object (1 fields) |  |
| `purpose` | yes | string |  |
| `released_at` | no | object (2 fields) |  |
| `settled_at` | no | object (2 fields) |  |
| `state` | yes | string |  |
| `updated_at` | yes | string |  |
| `work_document` | yes | object |  |
| `work_document_sha256` | yes | string |  |
| `work_id` | yes | string |  |
