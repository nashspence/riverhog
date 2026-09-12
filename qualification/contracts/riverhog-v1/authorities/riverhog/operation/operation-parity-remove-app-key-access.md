# Operation parity: remove_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-remove-app-key-access:b4f11a480b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/6`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [DELETE /v1/apps/{app}/keys/{key_id}/access](../http/delete-v1-apps-app-keys-key-id-access.md)
- [piggity app key access remove](../../piggity/cli/piggity-app-key-access-remove.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key access remove"] |
| `client` | ApiClient |
| `method` | DELETE |
| `operation_id` | remove_app_key_access |
| `path` | /v1/apps/{app}/keys/{key_id}/access |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 204b2eb6478fd030999853284413622b780548f95aa489dc610e1f1d6a60a166 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key access remove"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "remove_app_key_access",
  "path": "/v1/apps/{app}/keys/{key_id}/access",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
