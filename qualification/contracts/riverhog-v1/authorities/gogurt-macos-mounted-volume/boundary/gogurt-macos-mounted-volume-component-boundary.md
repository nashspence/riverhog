# gogurt-macos-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-component-boundary:b842309763 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-macos-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| `distribution` | "gogurt-macos-mounted-volume" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/mounted-volume/macos" |
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

- `/boundaries/components/20`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9640e3d7a5bac1acb70c260630c215d11815fafa1b182477da0157ed68671ef -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-macos-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/macos",
  "role": "reference_component"
}
```
