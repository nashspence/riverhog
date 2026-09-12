# Operation parity: ftp_adapter_health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-ftp-adapter-health-live:a8c347c2e7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `operation` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/109`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /health/live](../http/get-health-live.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog-ftp-adapter |
| `classification` | standard-tool/protocol |
| `cli_commands` | [] |
| `client` | RiverhogFtpAdapterClient |
| `method` | GET |
| `operation_id` | ftp_adapter_health_live |
| `path` | /health/live |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
