# riverhog-provenance-linux-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-contracts:riverhog-provenance-linux-contracts-compo-9fff7df012:2a32b7762b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/29`

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
| `distribution` | "riverhog-provenance-linux-contracts" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/provenance/contracts/linux" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55a45b71147e2ad0c9d7766f7a12bb793971b867e2c09887990e17b3113798eb -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-linux-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/linux",
  "role": "reference_component"
}
```
