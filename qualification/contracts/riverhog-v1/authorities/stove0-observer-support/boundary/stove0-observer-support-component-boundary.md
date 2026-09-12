# stove0-observer-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-support:stove0-observer-support-component-boundary:8fe570167c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/49`

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
| `console_scripts` | object (2 fields) |
| `dependencies` | array (4 items) |
| `distribution` | "stove0-observer-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/packages/observer-support" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76adbdbfbaa7c3989dbc9f0c557be0aa0b3a430876321d5bf9f4413a60f7ff41 -->

```json
{
  "console_scripts": {
    "stove0-observer-conformance": "stove0_observer_support.conformance:main",
    "stove0-observer-schemas": "stove0_observer_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "stove0-observer-client",
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-observer-support",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-support",
  "role": "reusable_library"
}
```
