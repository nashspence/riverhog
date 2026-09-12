# riverhog-provenance-windows-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-observer:riverhog-provenance-windows-observer-comp-f14e00eb8c:19842e4df0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/34`

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
| `distribution` | "riverhog-provenance-windows-observer" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/provenance/observers/windows" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6aa23de111a1bca534b3b3fa93e76add4beec70cce178419895befa744a1b8b9 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-windows-contracts"
  ],
  "distribution": "riverhog-provenance-windows-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/windows",
  "role": "reference_component"
}
```
