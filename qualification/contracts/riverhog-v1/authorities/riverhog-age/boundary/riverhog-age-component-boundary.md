# riverhog-age component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-age:riverhog-age-component-boundary:9702b630dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-age` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "riverhog-age" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-age" |
| `role` | "reusable_library" |

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

- `/boundaries/components/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a35c65ff8f57497522afa0971bbc89d8f634cac95720267a34c80d37c785e8fa -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-age",
  "optional_dependencies": {},
  "path": "packages/riverhog-age",
  "role": "reusable_library"
}
```
