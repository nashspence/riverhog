# Operation parity: list_evaluations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-evaluations:635290721c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [evaluations](families/evaluations/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d30e73fad8"></a>
| Concern | Contract |
|---|---|
| <a id="s-331b4eae37"></a>`application` | stove0 |
| <a id="s-d2efd001e4"></a>`classification` | human-cli+json |
| <a id="s-7c0e1013c2"></a>`cli_commands` | ["evaluation list"] |
| <a id="s-54b39babdd"></a>`client` | Stove0ApiClient |
| <a id="s-0287283223"></a>`method` | GET |
| <a id="s-fe3cec1873"></a>`operation_id` | list_evaluations |
| <a id="s-bb54774786"></a>`path` | /v1/evaluations |
| <a id="s-521c097650"></a>`provider_evidence` | None |
| <a id="s-82e7535293"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-d7b7366c26"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations](../http/get-v1-evaluations.md)
- [stove0-client evaluation list](../../stove0-client/cli/stove0-client-evaluation-list.md)

## Governing policies

- <a id="pa-8c26ae2b9b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-f791938242"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ff65dc6d13"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/124`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 865aeb16d697b8f80c2d4db80cc7c949b692c64da1006ff99c2d8581c38b1c0a -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "evaluation list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_evaluations",
  "path": "/v1/evaluations",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "operator-projection"
}
```
