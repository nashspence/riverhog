# RIVERHOG_FTP_ADAPTER_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-base-url:a30fa80299 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/41`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL` — `configuration-environment:RIVERHOG_FTP_ADAPTER_BASE_URL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_FTP_ADAPTER_BASE_URL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1e45cc4e25d2d36930ab7c3431031589fff494ddbd0e9ac7ad3ff6d6baa0647 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_BASE_URL"
}
```
