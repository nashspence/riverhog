# gogurt-core component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-core-component-boundary:c502c632b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-core` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["config-validation"] |
| `distribution` | "gogurt-core" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/packages/core" |
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

- `/boundaries/components/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a13dd0a99c9b2814ece9d86a5dfcb639be14fd3ccca9599d577f4d709cabb0cf -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation"
  ],
  "distribution": "gogurt-core",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/core",
  "role": "reusable_library"
}
```
