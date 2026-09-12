# stove0-target-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-client:stove0-target-client-component-boundary:f9de47e9b5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/53`

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
| `distribution` | "stove0-target-client" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/packages/target-client" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b201d8b2c97c9751893de9bae7b0cc41aa826a67938c92a5be9c7876ae888a13 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-client",
  "role": "reusable_library"
}
```
