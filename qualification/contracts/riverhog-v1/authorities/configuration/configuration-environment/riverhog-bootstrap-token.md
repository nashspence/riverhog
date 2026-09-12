# RIVERHOG_BOOTSTRAP_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-bootstrap-token:5ed79e01de -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/24`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN` — `configuration-environment:RIVERHOG_BOOTSTRAP_TOKEN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_BOOTSTRAP_TOKEN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc21aa0955fbe979d3969c62040325732a936d49d02cbca4ea6b687a8d4c8bc6 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_BOOTSTRAP_TOKEN"
}
```
