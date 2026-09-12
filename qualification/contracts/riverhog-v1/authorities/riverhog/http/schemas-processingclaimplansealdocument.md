# schemas: ProcessingClaimPlanSealDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimplansealdocument:c20777cee8 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanSealDocument`

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
| encoded-size | bytes | `contract_max` | maximum=16777216, reason=bounded-controller-evidence-envelope |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract

- `title`: ProcessingClaimPlanSealDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `controller_evidence` | yes | object |  |
| `controller_evidence_sha256` | yes | string |  |
| `execution_id` | yes | string |  |
| `fence` | yes | integer |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `retirement_grace_seconds` | no | integer |  |
| `retirement_policy` | no | string |  |
