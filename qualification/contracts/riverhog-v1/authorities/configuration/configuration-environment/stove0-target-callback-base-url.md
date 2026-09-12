# STOVE0_TARGET_CALLBACK_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-base-url:fe515c8353 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/114`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL` — `configuration-environment:STOVE0_TARGET_CALLBACK_BASE_URL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_TARGET_CALLBACK_BASE_URL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6383cec2626069a73b724ada91e14edf7d0a13b588c74f5fd488d1355a6b901 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_BASE_URL"
}
```
