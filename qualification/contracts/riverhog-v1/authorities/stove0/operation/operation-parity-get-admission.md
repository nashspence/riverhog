# Operation parity: get_admission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-admission:e1958e2993 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admissions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["admission show"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | get_admission |
| `path` | /v1/admissions/{admission_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions/{admission_id}](../http/get-v1-admissions-admission-id.md)
- [stove0 admission show](../cli/stove0-admission-show.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/122`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3a42d0b21023ba9f33fceb4516991d3decb42d3ee253c5ce4c281364e7c23ef -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_admission",
  "path": "/v1/admissions/{admission_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
