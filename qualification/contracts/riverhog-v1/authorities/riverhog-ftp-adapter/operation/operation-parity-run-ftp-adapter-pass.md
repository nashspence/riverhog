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

## Contract summary

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

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed458d5ef46788b6f27bc9567d45adb4de10dac8dbea5a300b70d0f9a5f4d0d9 -->

```json
{
  "application": "riverhog-ftp-adapter",
  "classification": "human-cli+json",
  "cli_commands": [
    "run"
  ],
  "client": "RiverhogFtpAdapterClient",
  "method": "POST",
  "operation_id": "run_ftp_adapter_pass",
  "path": "/v1/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
