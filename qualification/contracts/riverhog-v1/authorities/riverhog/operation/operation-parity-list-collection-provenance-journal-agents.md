# Operation parity: list_collection_provenance_journal_agents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-provenan-1783067aa3:0bc7c0b4f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d7e13b605d"></a>
| Concern | Contract |
|---|---|
| <a id="s-6d0fac0d3a"></a>`application` | riverhog |
| <a id="s-bbd37b0147"></a>`classification` | human-cli+json |
| <a id="s-eb7c94622a"></a>`cli_commands` | ["collection provenance agents"] |
| <a id="s-f0e60d8db0"></a>`client` | ApiClient |
| <a id="s-e685659984"></a>`method` | GET |
| <a id="s-dffe1128dc"></a>`operation_id` | list_collection_provenance_journal_agents |
| <a id="s-c95d2ae49e"></a>`path` | /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents |
| <a id="s-9a6c017e6d"></a>`provider_evidence` | None |
| <a id="s-8c3bc847e2"></a>`read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| <a id="s-153a9a8984"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents](../http/get-v1-collections-collection-id-provenance-journals-journal-id-agents.md)
- [piggity collection provenance agents](../../piggity/cli/piggity-collection-provenance-agents.md)

## Governing policies

- <a id="pa-aaacb13977"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-91e5b6ef0b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f50d0df5fc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/81`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d43a79812a981f960df9921c9567f9872dbd3b532d4129225b010e6d6f21e50 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection provenance agents"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_collection_provenance_journal_agents",
  "path": "/v1/collections/{collection_id}/provenance/journals/{journal_id}/agents",
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
