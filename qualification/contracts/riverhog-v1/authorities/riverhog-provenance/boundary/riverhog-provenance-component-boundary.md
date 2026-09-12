# riverhog-provenance component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance:riverhog-provenance-component-boundary:b8addbec20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["riverhog-provenance-contracts"] |
| `distribution` | "riverhog-provenance" |
| `optional_dependencies` | empty object |
| `path` | "packages/riverhog-provenance" |
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

- `/boundaries/components/8`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5fbd501af743c1d1bf20a1fdd4e410d8b73c65f41e1cbbfeadc5030ade31562 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance",
  "optional_dependencies": {},
  "path": "packages/riverhog-provenance",
  "role": "reusable_library"
}
```
