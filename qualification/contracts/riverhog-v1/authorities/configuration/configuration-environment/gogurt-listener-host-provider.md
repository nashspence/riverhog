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

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "GOGURT_LISTENER_HOST_PROVIDER" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6273237e3ee8e5df7f318ac6fb4283703610b5f162da6616cee1a6f779622a9 -->

```json
{
  "consumers": [
    "gogurt"
  ],
  "name": "GOGURT_LISTENER_HOST_PROVIDER"
}
```
