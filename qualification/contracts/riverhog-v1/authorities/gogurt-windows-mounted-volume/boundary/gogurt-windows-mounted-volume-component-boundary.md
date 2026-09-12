# gogurt-windows-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-component-boundary:45fa244e43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-windows-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| `distribution` | "gogurt-windows-mounted-volume" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/mounted-volume/windows" |
| `role` | "reference_component" |

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

- `/boundaries/components/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffe1b5f6da03b41a6f4e05633dbd3e31f7a6d468997353cd89a52998349ac72e -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-windows-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/windows",
  "role": "reference_component"
}
```
