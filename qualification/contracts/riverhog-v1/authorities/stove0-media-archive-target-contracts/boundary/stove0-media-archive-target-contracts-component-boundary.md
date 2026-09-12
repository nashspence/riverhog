# stove0-media-archive-target-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-com-aef15ecff2:77e2e60a26 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-media-archive-target-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/56`

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
| `dependencies` | array (2 items) |
| `distribution` | "stove0-media-archive-target-contracts" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/media-archive/contracts" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d6c64db666fd47ad3d5fb7c1ffac758cf73b304b33f0702cac385a0d294a99b -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-media-archive-target-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/media-archive/contracts",
  "role": "reference_component"
}
```
