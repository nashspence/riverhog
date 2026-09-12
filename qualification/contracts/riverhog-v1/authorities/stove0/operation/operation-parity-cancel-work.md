# Operation parity: cancel_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-cancel-work:0d5f400798 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a5b4c6dadb9c"></a>
| Concern | Contract |
|---|---|
| <a id="s-dd1e180bc9ba"></a>`application` | stove0 |
| <a id="s-8634d3b92927"></a>`classification` | human-cli+json |
| <a id="s-c029a89f51b8"></a>`cli_commands` | ["work cancel"] |
| <a id="s-3c276c62e01c"></a>`client` | Stove0ApiClient |
| <a id="s-44d947a84621"></a>`method` | POST |
| <a id="s-b6e93a371fbc"></a>`operation_id` | cancel_work |
| <a id="s-0a43500f07ba"></a>`path` | /v1/work/{work_id}/cancel |
| <a id="s-8604e63815fa"></a>`provider_evidence` | None |
| <a id="s-6b8aa8ac92e1"></a>`read_collection` | None |
| <a id="s-4ffebee3d533"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/cancel](../http/post-v1-work-work-id-cancel.md)
- [stove0 work cancel](../cli/stove0-work-cancel.md)

## Governing policies

- <a id="pa-00b61d215e4e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f98b5808f0a3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8c359af6629e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/142`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d7d7cc8c3f791ee185dc47321cd4fc3cf6478e28f83269cd4dbfe3dbdf5e2bb -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work cancel"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "cancel_work",
  "path": "/v1/work/{work_id}/cancel",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
