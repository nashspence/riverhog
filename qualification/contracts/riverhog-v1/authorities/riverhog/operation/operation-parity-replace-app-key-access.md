# Operation parity: replace_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-replace-app-key-access:69a23d7c4f -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `apps` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/8`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [PUT /v1/apps/{app}/keys/{key_id}/access](../http/put-v1-apps-app-keys-key-id-access.md)
- [piggity app key access set](../../piggity/cli/piggity-app-key-access-set.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key access set"] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | replace_app_key_access |
| `path` | /v1/apps/{app}/keys/{key_id}/access |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
