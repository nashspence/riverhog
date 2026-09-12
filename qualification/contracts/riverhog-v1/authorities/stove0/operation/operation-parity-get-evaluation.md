# Operation parity: get_evaluation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-evaluation:21df6ac1b6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1366d8c694"></a>
| Concern | Contract |
|---|---|
| <a id="s-ee5c555a77"></a>`application` | stove0 |
| <a id="s-d2b198d327"></a>`classification` | human-cli+json |
| <a id="s-646e617a45"></a>`cli_commands` | ["evaluation show"] |
| <a id="s-2321cfa3dc"></a>`client` | Stove0ApiClient |
| <a id="s-0fb3b7224d"></a>`method` | GET |
| <a id="s-128af81bd8"></a>`operation_id` | get_evaluation |
| <a id="s-493bcdc303"></a>`path` | /v1/evaluations/{evaluation_id} |
| <a id="s-69dc7ce027"></a>`provider_evidence` | None |
| <a id="s-e8bbf13ad8"></a>`read_collection` | None |
| <a id="s-839d00ff25"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations/{evaluation_id}](../http/get-v1-evaluations-evaluation-id.md)
- [stove0-client evaluation show](../../stove0-client/cli/stove0-client-evaluation-show.md)

## Governing policies

- <a id="pa-799f9100db"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-e69e227efd"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-2ae89e52ae"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/126`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d15c2996d1c311d94a76d209622f2054bd940647563ca3b1785e15150514a947 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_evaluation",
  "path": "/v1/evaluations/{evaluation_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
