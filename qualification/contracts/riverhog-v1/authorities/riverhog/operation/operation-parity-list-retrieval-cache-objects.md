# Operation parity: list_retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-retrieval-cache-objects:53add94057 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-cache` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/94`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/retrieval-cache/objects](../http/get-v1-retrieval-cache-objects.md)
- [piggity retrieval cache list](../../piggity/cli/piggity-retrieval-cache-list.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["retrieval cache list"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_retrieval_cache_objects |
| `path` | /v1/retrieval-cache/objects |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1926a5d532adc9194b9f2b2063f37706564b31283db1535fd9337eb2fba704dd -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_retrieval_cache_objects",
  "path": "/v1/retrieval-cache/objects",
  "provider_evidence": "provider-qualification:#442",
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
