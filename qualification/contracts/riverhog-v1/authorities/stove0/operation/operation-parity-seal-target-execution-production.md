# Operation parity: seal_target_execution_production

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-seal-target-execution-production:7c48da26f7 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `target-executions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/137`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/target-executions/{job_id}/production/seal](../http/post-v1-target-executions-job-id-production-seal.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | client-only-primitive |
| `cli_commands` | [] |
| `client` | TargetCallbackClient |
| `method` | POST |
| `operation_id` | seal_target_execution_production |
| `path` | /v1/target-executions/{job_id}/production/seal |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ceb164283fc6412ab65f338201fdf14249639616f42a803a4ad54729c9653fef -->

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "POST",
  "operation_id": "seal_target_execution_production",
  "path": "/v1/target-executions/{job_id}/production/seal",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
