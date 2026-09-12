# RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-estimated-latency:9b58d04b3e -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/64`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY` — `configuration-environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37f1a6575e0294f915e43a52e04a0133cbb80c7973d412a7e8393697207855c9 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY"
}
```
