# riverhog-provenance-windows-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-com-27ca113ded:d918bb9f73 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/31`

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
| `distribution` | "riverhog-provenance-windows-contracts" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/provenance/contracts/windows" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad1320843a2a320965505d05601c3bc58529d81a8b82441dfe70e520d3647a92 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-windows-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/windows",
  "role": "reference_component"
}
```
