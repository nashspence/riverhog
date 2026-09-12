# RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-event-context-reap-batch-size:d85fcf5925 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/configuration_environment/37`

## Effective policies

- `compatibility/configuration/v1`
- `extent-rule/configured-capacity/v1`

## Executable sources and proof

- `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE` — `configuration-environment:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE`
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
| `name` | "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd313c075766908be097411390f7e51b843be812fb38dbb6372f13164a15e97 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE"
}
```
