# GOGURT_MOUNTED_VOLUME_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:gogurt-mounted-volume-provider:706cf99e7d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/1`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER` — `configuration-environment:GOGURT_MOUNTED_VOLUME_PROVIDER`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_MOUNTED_VOLUME_PROVIDER"
}
```
