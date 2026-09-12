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

<a id="s-0d975baeaa"></a>
| Concern | Contract |
|---|---|
| <a id="s-7dfb39364e"></a>`application` | stove0 |
| <a id="s-9f572a8945"></a>`classification` | standard-tool/protocol |
| <a id="s-ed21cdc478"></a>`cli_commands` | ["health"] |
| <a id="s-0fbcaad8ad"></a>`client` | Stove0ApiClient |
| <a id="s-f7315a9035"></a>`method` | GET |
| <a id="s-e67f9e04e4"></a>`operation_id` | health_ready |
| <a id="s-ed97e48988"></a>`path` | /health/ready |
| <a id="s-77ef58b9b8"></a>`provider_evidence` | None |
| <a id="s-bdb06d5da7"></a>`read_collection` | None |
| <a id="s-24512ea65a"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/ready](../http/get-health-ready.md)
- [stove0-client health](../../stove0-client/cli/stove0-client-health.md)

## Governing policies

- <a id="pa-2f872db71d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ee79b405a4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3d4e248ff1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
