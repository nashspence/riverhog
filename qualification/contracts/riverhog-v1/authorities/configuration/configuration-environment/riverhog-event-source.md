# RIVERHOG_EVENT_SOURCE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-source:c7769bbb88 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/39`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_EVENT_SOURCE` — `configuration-environment:RIVERHOG_EVENT_SOURCE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "RIVERHOG_EVENT_SOURCE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 064ef6535473b031ad1e473acc663f9f48dd3ebc75e1c9ae3bba89fd19945756 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_SOURCE"
}
```
