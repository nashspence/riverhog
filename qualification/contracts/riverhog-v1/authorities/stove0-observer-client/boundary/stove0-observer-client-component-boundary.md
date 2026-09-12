# stove0-observer-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-client:stove0-observer-client-component-boundary:72a37bbc2a -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/47`

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
| `distribution` | "stove0-observer-client" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/packages/observer-client" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f00d43de862423fd4865909185cf11bada37089bc755da10a52994584b601856 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-observer-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-client",
  "role": "reusable_library"
}
```
