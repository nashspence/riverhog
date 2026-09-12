# Operation parity: get_target_execution_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-target-execution-inputs:b670142484 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | TargetCallbackClient |
| `method` | GET |
| `operation_id` | get_target_execution_inputs |
| `path` | /v1/target-executions/{job_id}/inputs |
| `provider_evidence` | None |
| `read_collection` | {"authority": "target-input-authority", "cursor_parameter": "continuation", "fixed_limit": 256, "kind": "exact-authority-page"} |
| `response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/target-executions/{job_id}/inputs](../http/get-v1-target-executions-job-id-inputs.md)

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

- `/external_contract/operations/135`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6086e30fcebe16289130b8161394f6e07c2e1584c5a6846af3aaba1257b4f388 -->

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "GET",
  "operation_id": "get_target_execution_inputs",
  "path": "/v1/target-executions/{job_id}/inputs",
  "provider_evidence": null,
  "read_collection": {
    "authority": "target-input-authority",
    "cursor_parameter": "continuation",
    "fixed_limit": 256,
    "kind": "exact-authority-page"
  },
  "response_authority": "canonical-document"
}
```
