# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-provenance-state-home:cdf1d02b3d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/55`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME` — `configuration-environment:RIVERHOG_PROVENANCE_STATE_HOME`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_PROVENANCE_STATE_HOME" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34353d7c2d47daaacfa034b08c1b2a693b0b397a3352533bf4e136d4f8ba48b3 -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "name": "RIVERHOG_PROVENANCE_STATE_HOME"
}
```
