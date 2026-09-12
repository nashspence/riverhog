# STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-source-revision:3e84450e57 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/100`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION` — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 067b79778e45e1bcebf559f08505f8bee3079c89a99d4e24eff39f31bf1223f2 -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION"
}
```
