# RIVERHOG_FTP_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-token:f9b3c800c8 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/45`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN` — `configuration-environment:RIVERHOG_FTP_ADAPTER_TOKEN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_FTP_ADAPTER_TOKEN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 738c8930bff67546a294f9b392414512b4711b5f54b2de19c5c6b67c78ba2018 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_TOKEN"
}
```
