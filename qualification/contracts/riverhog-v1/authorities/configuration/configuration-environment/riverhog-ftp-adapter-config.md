# RIVERHOG_FTP_ADAPTER_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-ftp-adapter-config:343b1f81d0 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/42`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG` — `configuration-environment:RIVERHOG_FTP_ADAPTER_CONFIG`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "name": "RIVERHOG_FTP_ADAPTER_CONFIG"
}
```
