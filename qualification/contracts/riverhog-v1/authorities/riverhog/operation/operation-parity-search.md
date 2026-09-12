# Operation parity: search

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-search:11c3e42d20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [search](families/search/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8345fc1c43"></a>
| Concern | Contract |
|---|---|
| <a id="s-8ad7dc5df5"></a>`application` | riverhog |
| <a id="s-a45144a7ef"></a>`classification` | human-cli+json |
| <a id="s-39261e0df6"></a>`cli_commands` | ["find"] |
| <a id="s-7871d67cfd"></a>`client` | ApiClient |
| <a id="s-bf95f602c1"></a>`method` | GET |
| <a id="s-77c9124679"></a>`operation_id` | search |
| <a id="s-3d3c0d3ca6"></a>`path` | /v1/search |
| <a id="s-db9f0587e9"></a>`provider_evidence` | None |
| <a id="s-c001b88794"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-a1a2efa6ea"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/search](../http/get-v1-search.md)
- [piggity find](../../piggity/cli/piggity-find.md)

## Governing policies

- <a id="pa-c14daeb83a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-d11bc5a386"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-277e8eb530"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/107`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd1235a7acb9b8976f40db6bf6a2708584cc1ba24b982942116b8e3b64d1dc6a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "find"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "search",
  "path": "/v1/search",
  "provider_evidence": null,
  "read_collection": {
    "default_page_size": 25,
    "kind": "mutable-browse",
    "maximum_page_size": 100,
    "next_page_token_field": "next_page_token",
    "page_size_parameter": "page_size",
    "page_token_parameter": "page_token"
  },
  "response_authority": "http-json"
}
```
