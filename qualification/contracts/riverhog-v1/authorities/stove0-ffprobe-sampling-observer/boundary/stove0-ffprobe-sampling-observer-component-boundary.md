# stove0-ffprobe-sampling-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-component-boundary:cfad371240 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-ffprobe-sampling-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/45`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (1 fields) |
| `dependencies` | array (4 items) |
| `distribution` | "stove0-ffprobe-sampling-observer" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/observers/ffprobe-sampling" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02a18b2c9ecd5777269d1183e2471301ce8871471a62f33fec09b4bfa681af72 -->

```json
{
  "console_scripts": {
    "stove0-ffprobe-sampling-observer": "stove0_ffprobe_sampling_observer.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-sampling-observer-contracts",
    "stove0-observer-protocol",
    "stove0-observer-support"
  ],
  "distribution": "stove0-ffprobe-sampling-observer",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/ffprobe-sampling",
  "role": "reference_component"
}
```
