# RIVERHOG_FTP_ADAPTER_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-http2:e133274d86 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/43`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2` — `configuration-environment:RIVERHOG_FTP_ADAPTER_HTTP2`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_FTP_ADAPTER_HTTP2" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cea2e99ea406e328870b3277e7759b8286ef84a8f5883ac5f200c6e1a09c3bd6 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_HTTP2"
}
```
