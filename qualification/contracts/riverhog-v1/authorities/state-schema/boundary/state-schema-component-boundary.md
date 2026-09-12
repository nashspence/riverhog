# state-schema component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:state-schema:state-schema-component-boundary:a60d84df07 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `state-schema` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | [] |
| `distribution` | "state-schema" |
| `optional_dependencies` | empty object |
| `path` | "packages/state-schema" |
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

- `/boundaries/components/13`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f77d2a929a73da9a7532ca1c1624c0e6c2257c2cbd33caf5c3fcc026ea80a50d -->

```json
{
  "console_scripts": {},
  "dependencies": [],
  "distribution": "state-schema",
  "optional_dependencies": {},
  "path": "packages/state-schema",
  "role": "internal_build_unit"
}
```
