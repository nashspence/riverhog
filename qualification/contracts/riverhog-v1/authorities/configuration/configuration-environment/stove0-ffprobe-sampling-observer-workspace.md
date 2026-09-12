# STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-workspace:e9b8df9d92 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/103`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE` — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c301b6b4fcc48e78c0ead5cf7db7769735e0fbf678d702bf701a23408a88ff83 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE"
}
```
