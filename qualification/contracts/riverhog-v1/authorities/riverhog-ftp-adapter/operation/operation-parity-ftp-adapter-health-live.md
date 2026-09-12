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

## Contract summary

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

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e3a9c1399bd595e3430922247dd8abe6605cf1528d5eb1086acaca7952bb9b0 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "standard-tool/protocol",
  "cli_commands": [],
  "client": "RiverhogFtpAdapterClient",
  "method": "GET",
  "operation_id": "ftp_adapter_health_live",
  "path": "/health/live",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
