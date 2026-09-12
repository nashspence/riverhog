# Operation parity: health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-health-live:c75d673fef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-22cfc89a31"></a>
| Concern | Contract |
|---|---|
| <a id="s-a89c3891d7"></a>`application` | stove0 |
| <a id="s-21604b36f4"></a>`classification` | standard-tool/protocol |
| <a id="s-9d7b477afa"></a>`cli_commands` | ["health"] |
| <a id="s-54db73dc2a"></a>`client` | Stove0ApiClient |
| <a id="s-12bc989b92"></a>`method` | GET |
| <a id="s-db3026f4df"></a>`operation_id` | health_live |
| <a id="s-cd1b385a13"></a>`path` | /health/live |
| <a id="s-8bfd7c67c6"></a>`provider_evidence` | None |
| <a id="s-7251138768"></a>`read_collection` | None |
| <a id="s-c19ec9b3d4"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/live](../http/get-health-live.md)
- [stove0-client health](../../stove0-client/cli/stove0-client-health.md)

## Governing policies

- <a id="pa-e53b730a31"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-dff2d8ae93"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-39953c16d0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/114`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3bb17dd1416005d0eedae12c18e64e483a0c55bcadd67848073db7c288008e5 -->

```json
{
  "application": "stove0",
  "classification": "standard-tool/protocol",
  "cli_commands": [
    "health"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
