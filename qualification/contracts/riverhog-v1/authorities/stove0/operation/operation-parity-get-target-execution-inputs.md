# Operation parity: get_target_execution_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-target-execution-inputs:b670142484 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [target-executions](families/target-executions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0749c5c12b"></a>
| Concern | Contract |
|---|---|
| <a id="s-f437590bd6"></a>`application` | stove0 |
| <a id="s-20e96cb2b0"></a>`classification` | client-only-primitive |
| <a id="s-1fa0a1bee3"></a>`cli_commands` | [] |
| <a id="s-3a4be6fce8"></a>`client` | TargetCallbackClient |
| <a id="s-1d42ec0d46"></a>`method` | GET |
| <a id="s-e9aa9fe923"></a>`operation_id` | get_target_execution_inputs |
| <a id="s-2f8251cbc1"></a>`path` | /v1/target-executions/{job_id}/inputs |
| <a id="s-9f77efb817"></a>`provider_evidence` | None |
| <a id="s-089959e0a0"></a>`read_collection` | {"authority": "target-input-authority", "cursor_parameter": "continuation", "fixed_limit": 256, "kind": "exact-authority-page"} |
| <a id="s-833162c2b3"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/target-executions/{job_id}/inputs](../http/get-v1-target-executions-job-id-inputs.md)

## Governing policies

- <a id="pa-5e1d6abec5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-05fdacf444"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f055a6b002"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
