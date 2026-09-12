# stove0-media-archive-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-archive-target-support:stove0-media-archive-target-support-compo-e239b6cb59:ca2b980897 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-media-archive-target-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/57`

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
| `dependencies` | array (6 items) |
| `distribution` | "stove0-media-archive-target-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/media-archive/support" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f49957f019e48f6a03bfc440cb60fd2a6e8863918ab79ca298c5e6305bb45cb1 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol",
    "stove0-media-archive-target-contracts",
    "stove0-media-metadata-observer-contracts",
    "stove0-observer-protocol",
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-media-archive-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/media-archive/support",
  "role": "reference_component"
}
```
