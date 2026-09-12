# Operation parity: search

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-search:11c3e42d20 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `search` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/107`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/search](../http/get-v1-search.md)
- [piggity find](../../piggity/cli/piggity-find.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["find"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | search |
| `path` | /v1/search |
| `provider_evidence` | None |
| `read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| `response_authority` | http-json |

## Complete owned contract

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
