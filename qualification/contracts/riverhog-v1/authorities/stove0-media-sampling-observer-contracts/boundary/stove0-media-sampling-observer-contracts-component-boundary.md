# stove0-media-sampling-observer-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-d85f92867b:a64052d707 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-media-sampling-observer-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/43`

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
| `console_scripts` | object (0 fields) |
| `dependencies` | array (1 items) |
| `distribution` | "stove0-media-sampling-observer-contracts" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/observers/contracts/media-sampling" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a0100d502012b6d31761faf7a969f8b5bdab3258bd65b0b62ce686466f05361 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-media-sampling-observer-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/contracts/media-sampling",
  "role": "reference_component"
}
```
