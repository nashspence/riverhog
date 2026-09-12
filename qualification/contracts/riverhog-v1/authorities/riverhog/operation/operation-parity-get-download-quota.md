# Operation parity: get_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-download-quota:f146167eeb -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `download-quota` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/90`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/download-quota](../http/get-v1-download-quota.md)
- [piggity app key quota show](../../piggity/cli/piggity-app-key-quota-show.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["app key quota show"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | get_download_quota |
| `path` | /v1/download-quota |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
