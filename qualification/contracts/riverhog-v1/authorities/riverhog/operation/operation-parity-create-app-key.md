# Operation parity: create_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-app-key:2e14bc16f3 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/5`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/apps/{app}/keys](../http/post-v1-apps-app-keys.md)
- [piggity app key create](../../piggity/cli/piggity-app-key-create.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key create"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | create_app_key |
| `path` | /v1/apps/{app}/keys |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bddbfa841df0908b1ed686ea31134595256a03378434c334859ca5e317513e1f -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key create"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_app_key",
  "path": "/v1/apps/{app}/keys",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
