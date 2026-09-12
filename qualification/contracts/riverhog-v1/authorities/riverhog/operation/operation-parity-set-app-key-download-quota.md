# Operation parity: set_app_key_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-set-app-key-download-quota:14201c493d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/9`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../http/put-v1-apps-app-keys-key-id-download-quota.md)
- [piggity app key quota set](../../piggity/cli/piggity-app-key-quota-set.md)

## Contract

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
