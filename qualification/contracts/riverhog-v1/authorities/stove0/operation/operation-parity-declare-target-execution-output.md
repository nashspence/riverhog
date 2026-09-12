# Operation parity: declare_target_execution_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-declare-target-execution-output:3f5796943f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-82be94ec95"></a>
| Concern | Contract |
|---|---|
| <a id="s-6bb303aa36"></a>`application` | stove0 |
| <a id="s-3397db1278"></a>`classification` | client-only-primitive |
| <a id="s-22afb8ec5d"></a>`cli_commands` | [] |
| <a id="s-1ac8733df9"></a>`client` | TargetCallbackClient |
| <a id="s-403b9b0227"></a>`method` | PUT |
| <a id="s-b789f4f284"></a>`operation_id` | declare_target_execution_output |
| <a id="s-a0bb74e821"></a>`path` | /v1/target-executions/{job_id}/outputs/{artifact_id} |
| <a id="s-9fc39b246d"></a>`provider_evidence` | None |
| <a id="s-7fdec4557c"></a>`read_collection` | None |
| <a id="s-7eba5159a6"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/target-executions/{job_id}/outputs/{artifact_id}](../http/put-v1-target-executions-job-id-outputs-artifact-id.md)

## Governing policies

- <a id="pa-e0e147ab4e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-30e83e1c77"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ca143baeb5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/136`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb8c0b81b782fe75e87000ce36330074edb19d65c93093a57707fb47da4e25d1 -->

```json
{
  "application": "stove0",
  "classification": "client-only-primitive",
  "cli_commands": [],
  "client": "TargetCallbackClient",
  "method": "PUT",
  "operation_id": "declare_target_execution_output",
  "path": "/v1/target-executions/{job_id}/outputs/{artifact_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
