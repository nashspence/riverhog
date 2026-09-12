# Operation parity: set_app_key_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-set-app-key-download-quota:14201c493d -->

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
| `cli_commands` | ["app key quota set"] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | set_app_key_download_quota |
| `path` | /v1/apps/{app}/keys/{key_id}/download-quota |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../http/put-v1-apps-app-keys-key-id-download-quota.md)
- [piggity app key quota set](../../piggity/cli/piggity-app-key-quota-set.md)

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

- `/external_contract/operations/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2a4ce7aa2243bbe80d31d61af9830710799fe00be7d579d1d2cca175d3ece47 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "app key quota set"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "set_app_key_download_quota",
  "path": "/v1/apps/{app}/keys/{key_id}/download-quota",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
