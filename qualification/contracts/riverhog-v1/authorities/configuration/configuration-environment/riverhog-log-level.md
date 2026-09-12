# RIVERHOG_LOG_LEVEL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-log-level:dc7b9c626b -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/51`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_LOG_LEVEL` — `configuration-environment:RIVERHOG_LOG_LEVEL`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_LOG_LEVEL" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43687f68407eb01083f0d7e98e6f52a4e8650852fdfe452cb07dc905ab208814 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_LOG_LEVEL"
}
```
