# RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-cache-sweep-interval:ddd4870015 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/61`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL` — `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | configured-value | `operational_policy` | maximum=None, reason=operator-configured-capacity |

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2402ae87bc61e0e0d06dfd760dd97369473b1086c7530b778fa79826c5b185f3 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL"
}
```
