# Operation parity: declare_target_execution_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-declare-target-execution-db43bd94b6:8f28ff78d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7ca0e837ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-d78f536876"></a>`application` | stove0 |
| <a id="s-383e70feef"></a>`classification` | client-only-primitive |
| <a id="s-af78fc8ff7"></a>`cli_commands` | [] |
| <a id="s-ada9f5efda"></a>`client` | TargetCallbackClient |
| <a id="s-dddc09ca42"></a>`method` | PUT |
| <a id="s-93c47d53cc"></a>`operation_id` | declare_target_execution_disposition |
| <a id="s-8f02fd5ac9"></a>`path` | /v1/target-executions/{job_id}/dispositions/{input_id} |
| <a id="s-f4ffda5890"></a>`provider_evidence` | None |
| <a id="s-b8823d2d34"></a>`read_collection` | None |
| <a id="s-c22f349521"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/target-executions/{job_id}/dispositions/{input_id}](../http/put-v1-target-executions-job-id-dispositions-input-id.md)

## Governing policies

- <a id="pa-cc7d93b66e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d828d60183"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-9d8892726a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/134`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25589b56bc832d7a720958a9dfdaad2ff42296044dde3dc9c296799681e51256 -->

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "PUT",
  "operation_id": "declare_target_execution_disposition",
  "path": "/v1/target-executions/{job_id}/dispositions/{input_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
