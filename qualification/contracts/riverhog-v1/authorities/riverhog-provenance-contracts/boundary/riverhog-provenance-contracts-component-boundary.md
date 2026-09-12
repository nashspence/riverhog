# riverhog-provenance-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts-component-boundary:6aca73c088 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "riverhog-provenance-contracts" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-provenance-contracts" |
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

- `/boundaries/components/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 080d2dd485fdb63a71cb0353bc89990b80cf8e951ff217e300e0c3b0ac752637 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "riverhog-provenance-contracts",
  "optional_dependencies": {},
  "path": "packages/riverhog-provenance-contracts",
  "role": "reusable_library"
}
```
