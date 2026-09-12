# Operation parity: list_collection_provenance_journal_agents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-collection-provenan-1783067aa3:0bc7c0b4f7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collections` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/81`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents](../http/get-v1-collections-collection-id-provenance-journals-journal-id-agents.md)
- [piggity collection provenance agents](../../piggity/cli/piggity-collection-provenance-agents.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["collection provenance agents"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_collection_provenance_journal_agents |
| `path` | /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents |
| `provider_evidence` | None |
| `read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| `response_authority` | http-json |

## Complete owned contract

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
