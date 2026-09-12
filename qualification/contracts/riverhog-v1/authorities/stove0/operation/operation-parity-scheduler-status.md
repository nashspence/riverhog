# Operation parity: scheduler_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-scheduler-status:f8b1a09d22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admin` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["scheduler status"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | scheduler_status |
| `path` | /v1/admin/scheduler |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/admin/scheduler](../http/get-v1-admin-scheduler.md)
- [stove0 scheduler status](../cli/stove0-scheduler-status.md)

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

- `/external_contract/operations/116`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae0201cec0085f087322403983e0a8d2926ee73c04fd8dc18356d4d2e7dc2b31 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "scheduler status"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "scheduler_status",
  "path": "/v1/admin/scheduler",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
