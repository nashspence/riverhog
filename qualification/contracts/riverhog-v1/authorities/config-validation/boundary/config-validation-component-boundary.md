# config-validation component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:config-validation:config-validation-component-boundary:dcc8a2932b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `config-validation` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "config-validation" |
| `optional_dependencies` | empty object |
| `path` | "packages/config-validation" |
| `role` | "internal_build_unit" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/components/0`

### Exact owned JSON

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
