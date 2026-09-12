# Operation parity: ftp_adapter_health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-ftp-adapter-health-ready:2f9fc73012 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `operation` |
| Family | `health` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/110`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /health/ready](../http/get-health-ready.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog-ftp-adapter |
| `classification` | standard-tool/protocol |
| `cli_commands` | [] |
| `client` | RiverhogFtpAdapterClient |
| `method` | GET |
| `operation_id` | ftp_adapter_health_ready |
| `path` | /health/ready |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7a2208a1125494f199c490a54dc5b49f39bd920d0de5d97b8bedd86ec4fb87 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "ftp_adapter_health_ready",
  "path": "/health/ready",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
