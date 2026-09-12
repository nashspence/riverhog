# Operation parity: run_scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-run-scheduler:11193d6199 -->

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
| `cli_commands` | ["scheduler run"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | run_scheduler |
| `path` | /v1/admin/scheduler/run |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admin/scheduler/run](../http/post-v1-admin-scheduler-run.md)
- [stove0 scheduler run](../cli/stove0-scheduler-run.md)

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

- `/external_contract/operations/117`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37067a8669a5f1333adfc5b51cb4f919826b1256e84020265443093a6ca72bd7 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "scheduler run"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "run_scheduler",
  "path": "/v1/admin/scheduler/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
