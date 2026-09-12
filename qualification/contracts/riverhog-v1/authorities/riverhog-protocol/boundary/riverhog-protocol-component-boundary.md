# riverhog-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-protocol:riverhog-protocol-component-boundary:3737face3c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/7`

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
| `distribution` | "riverhog-protocol" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/riverhog-protocol" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90b954e7efb6f9f8d9a0ed08063211e33ac04c9f992ac100ae80b01cacfc4c44 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-protocol",
  "role": "reusable_library"
}
```
