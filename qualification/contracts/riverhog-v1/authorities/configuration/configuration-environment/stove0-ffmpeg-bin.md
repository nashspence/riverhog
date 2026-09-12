# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffmpeg-bin:4d8a7bb05d -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/95`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFMPEG_BIN` — `configuration-environment:STOVE0_FFMPEG_BIN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (4 items) |
| `name` | "STOVE0_FFMPEG_BIN" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 413f1a9293c253aeb8b41eb3dcc1501edcd3bc1265a39ef4b2571b643c7da447 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler",
    "stove0-nvenc-av1-opus-target",
    "stove0-opus-review-sampler",
    "stove0-opus-target"
  ],
  "name": "STOVE0_FFMPEG_BIN"
}
```
