# Operation parity: list_admission_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-admission-policies:1dff9fb756 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admission-policies` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/118`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/admission-policies](../http/get-v1-admission-policies.md)
- [stove0 admission policy list](../cli/stove0-admission-policy-list.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["admission policy list"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | list_admission_policies |
| `path` | /v1/admission-policies |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |
