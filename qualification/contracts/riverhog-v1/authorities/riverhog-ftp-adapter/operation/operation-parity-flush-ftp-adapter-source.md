# Operation parity: flush_ftp_adapter_source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog-ftp-adapter:operation-parity-flush-ftp-adapter-source:aa0dc84b87 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `operation` |
| Family | `sources` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/112`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/sources/{source_id}/flush](../http/post-v1-sources-source-id-flush.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog-ftp-adapter |
| `classification` | human-cli+json |
| `cli_commands` | ["flush"] |
| `client` | RiverhogFtpAdapterClient |
| `method` | POST |
| `operation_id` | flush_ftp_adapter_source |
| `path` | /v1/sources/{source_id}/flush |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df9f25bf18577e80f9f417ed9806ca4716395dd67a2028700fd2a7be09f6be99 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "flush"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "flush_ftp_adapter_source",
  "path": "/v1/sources/{source_id}/flush",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
