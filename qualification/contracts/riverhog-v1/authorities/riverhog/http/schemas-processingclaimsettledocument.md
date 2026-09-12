# schemas: ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimsettledocument:81a030c9d6 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimSettleDocument`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

- `title`: ProcessingClaimSettleDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `derivation` | yes | #/components/schemas/CollectionDerivationDocument |  |
| `fence` | yes | integer |  |
| `outcome` | no | object (1 fields) |  |
| `output_collection_id` | yes | #/components/schemas/CollectionId |  |
