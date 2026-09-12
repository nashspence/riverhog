# Operation parity: health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-health-ready:0b492527d9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0d975baeaa8f"></a>
| Concern | Contract |
|---|---|
| <a id="s-7dfb39364e98"></a>`application` | stove0 |
| <a id="s-9f572a89452a"></a>`classification` | standard-tool/protocol |
| <a id="s-ed21cdc4785f"></a>`cli_commands` | ["health"] |
| <a id="s-0fbcaad8adb5"></a>`client` | Stove0ApiClient |
| <a id="s-f7315a903520"></a>`method` | GET |
| <a id="s-e67f9e04e46d"></a>`operation_id` | health_ready |
| <a id="s-ed97e48988e9"></a>`path` | /health/ready |
| <a id="s-77ef58b9b8c4"></a>`provider_evidence` | None |
| <a id="s-bdb06d5da7c9"></a>`read_collection` | None |
| <a id="s-24512ea65a19"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/ready](../http/get-health-ready.md)
- [stove0 health](../cli/stove0-health.md)

## Governing policies

- <a id="pa-2f872db71dca"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-ee79b405a435"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-3d4e248ff154"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/115`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60cebda6865bee636c94554b29e742fda4bde6bbfa1b36a6bf7cbd8b0fa51fd9 -->

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
