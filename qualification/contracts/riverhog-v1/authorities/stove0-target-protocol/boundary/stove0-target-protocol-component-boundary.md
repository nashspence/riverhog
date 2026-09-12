# stove0-target-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-protocol:stove0-target-protocol-component-boundary:ad5ba6edfb -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/54`

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
| `dependencies` | array (3 items) |
| `distribution` | "stove0-target-protocol" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/packages/target-protocol" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3745bf97c2ecbf800973bd9913d326f91011bf5844db09f9badfab02e36eb724 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-protocol",
    "stove0-protocol"
  ],
  "distribution": "stove0-target-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-protocol",
  "role": "reusable_library"
}
```
