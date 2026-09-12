# riverhog-archive-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-archive-contracts:riverhog-archive-contracts-component-boundary:8d0029d3a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "riverhog-archive-contracts" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-archive-contracts" |
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

- `/boundaries/components/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0034421b90dec670862cab707c555184090fde2a5478db69424ad8e16678b9d -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-archive-contracts",
  "optional_dependencies": {},
  "path": "packages/riverhog-archive-contracts",
  "role": "reusable_library"
}
```
