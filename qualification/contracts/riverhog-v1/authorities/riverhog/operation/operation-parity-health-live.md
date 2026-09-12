# Operation parity: health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-health-live:a8f70c3c13 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [health](families/health/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-13ca49c17f06"></a>
| Concern | Contract |
|---|---|
| <a id="s-a7da7dd6364a"></a>`application` | riverhog |
| <a id="s-1b73d7ce2c25"></a>`classification` | standard-tool/protocol |
| <a id="s-4c458192a061"></a>`cli_commands` | [] |
| <a id="s-2c87316e7739"></a>`client` | None |
| <a id="s-96d51d39ebf6"></a>`method` | GET |
| <a id="s-8da3990bc605"></a>`operation_id` | health_live |
| <a id="s-7b55f2cde69b"></a>`path` | /health/live |
| <a id="s-2a735f1af0a2"></a>`provider_evidence` | None |
| <a id="s-b445e8d13adc"></a>`read_collection` | None |
| <a id="s-10ec4136b0ab"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /health/live](../http/get-health-live.md)

## Governing policies

- <a id="pa-9f9bafa9b834"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-4c55bd3ab7fc"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-8084eaad39d6"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 69366580c4dcb0ec633c62eabf8bd22f06606bd0b4b667c6b4675dade29fab98 -->

```json
{
  "application": "riverhog",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": null,
  "method": "GET",
  "operation_id": "health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
