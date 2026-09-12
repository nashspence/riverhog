# time-formats component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:time-formats:time-formats-component-boundary:b09c1ef5eb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `time-formats` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "time-formats" |
| `optional_dependencies` | empty object |
| `path` | "packages/time-formats" |
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

- `/boundaries/components/14`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7af59e30a3a75be2fd9ddc30d73e4f3a8e6ace25b88fe9409cfddfb81c60e48 -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "time-formats",
  "optional_dependencies": {},
  "path": "packages/time-formats",
  "role": "internal_build_unit"
}
```
