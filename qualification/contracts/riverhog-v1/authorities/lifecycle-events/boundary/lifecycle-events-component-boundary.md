# lifecycle-events component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:lifecycle-events:lifecycle-events-component-boundary:dc11b5cdd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `lifecycle-events` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["time-formats"] |
| `distribution` | "lifecycle-events" |
| `optional_dependencies` | empty object |
| `path` | "packages/lifecycle-events" |
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

- `/boundaries/components/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cfa9a09d7963dbd01e57766f30b10ed5f50d9882ddb3ecd5ec78d8af532b5cc8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "time-formats"
  ],
  "distribution": "lifecycle-events",
  "optional_dependencies": {},
  "path": "packages/lifecycle-events",
  "role": "reusable_library"
}
```
