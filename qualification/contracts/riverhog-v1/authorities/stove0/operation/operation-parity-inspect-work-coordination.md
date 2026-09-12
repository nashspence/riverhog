# Operation parity: inspect_work_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-inspect-work-coordination:76362d78bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-938f87c08987"></a>
| Concern | Contract |
|---|---|
| <a id="s-b083cfdea1e5"></a>`application` | stove0 |
| <a id="s-3fc98de3c119"></a>`classification` | human-cli+json |
| <a id="s-e3f616a03dd4"></a>`cli_commands` | ["work coordination"] |
| <a id="s-e2eb4f77b5da"></a>`client` | Stove0ApiClient |
| <a id="s-dccdbe91febe"></a>`method` | GET |
| <a id="s-12ff092a6dd8"></a>`operation_id` | inspect_work_coordination |
| <a id="s-fe5e6e0665fb"></a>`path` | /v1/work/{work_id}/coordination |
| <a id="s-b3e574a397c4"></a>`provider_evidence` | None |
| <a id="s-7218fc4fa124"></a>`read_collection` | None |
| <a id="s-446a86dd1e9d"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}/coordination](../http/get-v1-work-work-id-coordination.md)
- [stove0 work coordination](../cli/stove0-work-coordination.md)

## Governing policies

- <a id="pa-dd8b84ae8394"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-b4aac39f30f8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-771b78012b53"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/143`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7a1df3778cbea5bb1aa22e8bfba8b0c4bec6cc63150813dc36b7e5d4e93c336 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work coordination"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "inspect_work_coordination",
  "path": "/v1/work/{work_id}/coordination",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
