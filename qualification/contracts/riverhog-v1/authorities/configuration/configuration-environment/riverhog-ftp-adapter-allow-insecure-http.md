# RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-allow-insecure-http:0f02bf74a5 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/40`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP` — `configuration-environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbc27c92dfb581e67fb969bb3f829f36479487be2343d8974725c0a3b0000706 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP"
}
```
