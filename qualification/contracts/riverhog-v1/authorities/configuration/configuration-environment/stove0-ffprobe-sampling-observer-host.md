# STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-host:953d8d15ee -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/97`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST` — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 809719e9cc2c52ef8ae6d5226b96fbeeaccd269bd261f12154475b8affc525d6 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST"
}
```
