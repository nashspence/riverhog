# Operation parity: list_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-list-work:6f2e0f4220 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [work](families/work/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-399455cff5"></a>
| Concern | Contract |
|---|---|
| <a id="s-16a4ab63ea"></a>`application` | stove0 |
| <a id="s-b5d9bf82a0"></a>`classification` | human-cli+json |
| <a id="s-10ab64710e"></a>`cli_commands` | ["work list"] |
| <a id="s-155b306c51"></a>`client` | Stove0ApiClient |
| <a id="s-c844917121"></a>`method` | GET |
| <a id="s-a8c3e0a80c"></a>`operation_id` | list_work |
| <a id="s-a6a5fb576a"></a>`path` | /v1/work |
| <a id="s-73c36608c3"></a>`provider_evidence` | None |
| <a id="s-75b7b4eaa6"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-d78942cc1d"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [GET /v1/work](../http/get-v1-work.md)
- [stove0 work list](../cli/stove0-work-list.md)

## Governing policies

- <a id="pa-b559563723"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-60d85b8b27"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b789f1b942"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/139`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c5bb919ac474e184b8866fe94cac4d93512a403005859bc337cbf247b6fa49d -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work list"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "list_work",
  "path": "/v1/work",
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
