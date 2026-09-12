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

<a id="s-22cfc89a311f"></a>
| Concern | Contract |
|---|---|
| <a id="s-a89c3891d71a"></a>`application` | stove0 |
| <a id="s-21604b36f4d3"></a>`classification` | standard-tool/protocol |
| <a id="s-9d7b477afabc"></a>`cli_commands` | ["health"] |
| <a id="s-54db73dc2abd"></a>`client` | Stove0ApiClient |
| <a id="s-12bc989b9255"></a>`method` | GET |
| <a id="s-db3026f4dfff"></a>`operation_id` | health_live |
| <a id="s-cd1b385a13d2"></a>`path` | /health/live |
| <a id="s-8bfd7c67c69e"></a>`provider_evidence` | None |
| <a id="s-725113876804"></a>`read_collection` | None |
| <a id="s-c19ec9b3d4b4"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/live](../http/get-health-live.md)
- [stove0 health](../cli/stove0-health.md)

## Governing policies

- <a id="pa-e53b730a31b2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-dff2d8ae936c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-39953c16d0b2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
