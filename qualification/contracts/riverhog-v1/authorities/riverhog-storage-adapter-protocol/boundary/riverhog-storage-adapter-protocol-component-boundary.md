# riverhog-storage-adapter-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-component-boundary:161627301d -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/11`

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
| `distribution` | "riverhog-storage-adapter-protocol" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/riverhog-storage-adapter-protocol" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 632e07c9f235fe41e4baaed295768b2c99f0e8a27458437b82f4838a7767afb9 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-protocol",
  "optional_dependencies": {},
  "path": "packages/riverhog-storage-adapter-protocol",
  "role": "reusable_library"
}
```
