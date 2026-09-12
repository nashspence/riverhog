# Operation parity: run_ftp_adapter_pass

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-run-ftp-adapter-pass:51f48df72d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `operation` |
| Family | `run` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/111`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/run](../http/post-v1-run.md)

## Contract

| Concern | Contract |
|---|---|
| `application` | riverhog-ftp-adapter |
| `classification` | human-cli+json |
| `cli_commands` | ["run"] |
| `client` | RiverhogFtpAdapterClient |
| `method` | POST |
| `operation_id` | run_ftp_adapter_pass |
| `path` | /v1/run |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |
