# Operation parity: declare_target_execution_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-declare-target-execution-72f1fb6dc1:78781fb31d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8e71f72499"></a>
| Concern | Contract |
|---|---|
| <a id="s-684bd7a9d7"></a>`application` | stove0 |
| <a id="s-801b3a5cc2"></a>`classification` | client-only-primitive |
| <a id="s-04348d7b72"></a>`cli_commands` | [] |
| <a id="s-60b4036f74"></a>`client` | TargetCallbackClient |
| <a id="s-1699b5ae4f"></a>`method` | PUT |
| <a id="s-990a5ad767"></a>`operation_id` | declare_target_execution_source_edge |
| <a id="s-e9c76191ba"></a>`path` | /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id} |
| <a id="s-1321756653"></a>`provider_evidence` | None |
| <a id="s-1f19a20d84"></a>`read_collection` | None |
| <a id="s-a853022ec1"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}](../http/put-v1-target-executions-job-id-source-edges-output-id-input-id.md)

## Governing policies

- <a id="pa-9d4d732cf4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-b48743dce2"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ea44c55d26"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/138`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1dd127ead922cc2bbbcc319291b27f974d873982a39ea56658c7d08dd63476fb -->

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "PUT",
  "operation_id": "declare_target_execution_source_edge",
  "path": "/v1/target-executions/{job_id}/source-edges/{output_id}/{input_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
