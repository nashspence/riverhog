# STOVE0_TARGET_CALLBACK_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-signing-key:294fd91c5d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/115`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY` — `configuration-environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_TARGET_CALLBACK_SIGNING_KEY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af62be45be5635c683d4dfc5eab89364d51ff6b43fdd055a2a6a6645457e52d5 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_SIGNING_KEY"
}
```
