# schemas: ProcessingClaimPlanDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimplandocument:0e187c388f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanDocument`

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
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |

## Contract

- `title`: ProcessingClaimPlanDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifacts` | yes | #/components/schemas/ArtifactSetAuthorityDocument |  |
| `controller_evidence` | yes | object |  |
| `controller_evidence_sha256` | yes | string |  |
| `execution_id` | yes | string |  |
| `inputs` | yes | #/components/schemas/ExactSetAuthorityDocument |  |
| `operation` | yes | #/components/schemas/OperationIdentityDocument |  |
| `retirement_grace_seconds` | yes | integer |  |
| `retirement_policy` | yes | string |  |
| `sealed_at` | yes | string |  |
