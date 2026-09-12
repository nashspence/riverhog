# riverhog-provenance-macos-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-macos-observer:riverhog-provenance-macos-observer-compon-f6fe932ece:958fc43472 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/33`

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
| `distribution` | "riverhog-provenance-macos-observer" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/provenance/observers/macos" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a28689f84efafe5040c4dd714ea3b438ba0ece129602437923f37bd6c969a23 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-macos-contracts"
  ],
  "distribution": "riverhog-provenance-macos-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/macos",
  "role": "reference_component"
}
```
