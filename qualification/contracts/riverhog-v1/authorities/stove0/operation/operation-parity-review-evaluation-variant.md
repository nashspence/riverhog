# Operation parity: review_evaluation_variant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-review-evaluation-variant:1a5900cfb5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `evaluations` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/130`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../http/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [stove0 evaluation review](../cli/stove0-evaluation-review.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["evaluation review"] |
| `client` | Stove0ApiClient |
| `method` | PUT |
| `operation_id` | review_evaluation_variant |
| `path` | /v1/evaluations/{evaluation_id}/variants/{variant_id}/review |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
