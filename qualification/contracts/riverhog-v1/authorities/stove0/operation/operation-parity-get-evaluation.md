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

<a id="s-1366d8c69482"></a>
| Concern | Contract |
|---|---|
| <a id="s-ee5c555a77a6"></a>`application` | stove0 |
| <a id="s-d2b198d327a8"></a>`classification` | human-cli+json |
| <a id="s-646e617a4509"></a>`cli_commands` | ["evaluation show"] |
| <a id="s-2321cfa3dc8e"></a>`client` | Stove0ApiClient |
| <a id="s-0fb3b7224ddc"></a>`method` | GET |
| <a id="s-128af81bd8c5"></a>`operation_id` | get_evaluation |
| <a id="s-493bcdc303a9"></a>`path` | /v1/evaluations/{evaluation_id} |
| <a id="s-69dc7ce027c8"></a>`provider_evidence` | None |
| <a id="s-e8bbf13ad8fc"></a>`read_collection` | None |
| <a id="s-839d00ff25e8"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations/{evaluation_id}](../http/get-v1-evaluations-evaluation-id.md)
- [stove0 evaluation show](../cli/stove0-evaluation-show.md)

## Governing policies

- <a id="pa-799f9100dbf9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-e69e227efd29"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-2ae89e52ae1a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

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
