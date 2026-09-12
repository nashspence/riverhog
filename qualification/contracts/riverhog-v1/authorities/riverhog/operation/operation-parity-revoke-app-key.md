# Operation parity: revoke_app_key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-revoke-app-key:9d2de65ee0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key revoke"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | revoke_app_key |
| `path` | /v1/apps/{app}/keys/{key_id}/revoke |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/apps/{app}/keys/{key_id}/revoke](../http/post-v1-apps-app-keys-key-id-revoke.md)
- [piggity app key revoke](../../piggity/cli/piggity-app-key-revoke.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/10`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c03a8a21f91f1a7f6444dfa8dc3bffcec1a04e13fd32ae055b912ebc48696cc0 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key revoke"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "revoke_app_key",
  "path": "/v1/apps/{app}/keys/{key_id}/revoke",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
