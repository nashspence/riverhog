# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-token:c9cf9e6b2e -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/101`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN` — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0d251d0a2d60388477f04c9d7df9af9230c5071ea6adb13a1ae023002772adc -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN"
}
```
