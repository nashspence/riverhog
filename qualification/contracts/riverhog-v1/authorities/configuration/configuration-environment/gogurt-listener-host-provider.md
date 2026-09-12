# GOGURT_LISTENER_HOST_PROVIDER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:gogurt-listener-host-provider:fc28a3e8f2 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/0`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER` — `configuration-environment:GOGURT_LISTENER_HOST_PROVIDER`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_LISTENER_HOST_PROVIDER"
}
```
