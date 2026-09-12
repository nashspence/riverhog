# config-validation component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:config-validation:config-validation-component-boundary:dcc8a2932b -->

| Audit field | Value |
|---|---|
| Authority | `config-validation` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/0`

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
| `dependencies` | array (0 items) |
| `distribution` | "config-validation" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/config-validation" |
| `role` | "internal_build_unit" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b43a98542be86ea9ac8eadce3ce534ade6681802cc05dda9bd5f9062cf4ccfe3 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "config-validation",
  "optional_dependencies": {},
  "path": "packages/config-validation",
  "role": "internal_build_unit"
}
```
