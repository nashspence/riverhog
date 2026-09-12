# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-browse-token-signing-key:cbbb4d1111 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/83`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY` — `configuration-environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_BROWSE_TOKEN_SIGNING_KEY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21ede63a620e23233fefa4f6acf5b4b7d3813b8be0a6ca8aa8a7d86f5fcd9cef -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY"
}
```
