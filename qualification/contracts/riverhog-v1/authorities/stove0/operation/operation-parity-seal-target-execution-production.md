# Operation parity: seal_target_execution_production

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-seal-target-execution-production:7c48da26f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-da441905862f"></a>
| Concern | Contract |
|---|---|
| <a id="s-49fc03c66df9"></a>`application` | stove0 |
| <a id="s-b1842ccd0e6f"></a>`classification` | client-only-primitive |
| <a id="s-1dd02a6a772f"></a>`cli_commands` | [] |
| <a id="s-d974d9d89c8b"></a>`client` | TargetCallbackClient |
| <a id="s-f950b484d22e"></a>`method` | POST |
| <a id="s-3cb728ea7ea0"></a>`operation_id` | seal_target_execution_production |
| <a id="s-4c0ddbebd7d2"></a>`path` | /v1/target-executions/{job_id}/production/seal |
| <a id="s-d4a4aa2b5b9d"></a>`provider_evidence` | None |
| <a id="s-d2f27f258b8e"></a>`read_collection` | None |
| <a id="s-af49082a9061"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [POST /v1/target-executions/{job_id}/production/seal](../http/post-v1-target-executions-job-id-production-seal.md)

## Governing policies

- <a id="pa-2e25f9e29140"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-eef8f704cc16"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-c6c912d88775"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/137`

### Exact owned JSON

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
