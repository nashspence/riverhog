# stove0-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-protocol:stove0-protocol-component-boundary:4c1f816eb1 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/51`

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
| `distribution` | "stove0-protocol" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/packages/protocol" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f98eb3abcca65308b8f58d79cdfb5401b551d8cbe5c98361d71ea613c7350fcc -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "stove0-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/protocol",
  "role": "reusable_library"
}
```
