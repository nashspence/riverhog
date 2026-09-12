# Operation parity: list_download_quotas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-download-quotas:b2e37de51f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `download-quotas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/91`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/download-quotas](../http/get-v1-download-quotas.md)
- [piggity app key quota list](../../piggity/cli/piggity-app-key-quota-list.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key quota list"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_download_quotas |
| `path` | /v1/download-quotas |
| `provider_evidence` | None |
| `read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a32f8d22b1f745a6a8ad4e74c75a318a57c1d25b48babfec0648eb8859ee9f0a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_download_quotas",
  "path": "/v1/download-quotas",
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
