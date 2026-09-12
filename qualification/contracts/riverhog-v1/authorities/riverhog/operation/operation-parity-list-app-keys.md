# Operation parity: list_app_keys

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-list-app-keys:48d9860661 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/4`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/apps/{app}/keys](../http/get-v1-apps-app-keys.md)
- [piggity app key list](../../piggity/cli/piggity-app-key-list.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key list"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | list_app_keys |
| `path` | /v1/apps/{app}/keys |
| `provider_evidence` | None |
| `read_collection` | {"default_page_size": 25, "kind": "mutable-browse", "maximum_page_size": 100, "next_page_token_field": "next_page_token", "page_size_parameter": "page_size", "page_token_parameter": "page_token"} |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 885a61841dc5109beb79feaf2d8bf9fba3cec47b56c4380588e5afb89c103666 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key list"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "list_app_keys",
  "path": "/v1/apps/{app}/keys",
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
