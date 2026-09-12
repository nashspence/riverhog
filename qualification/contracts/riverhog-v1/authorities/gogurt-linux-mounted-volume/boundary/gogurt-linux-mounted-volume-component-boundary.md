# gogurt-linux-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-component-boundary:e47d4ab54c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-linux-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["gogurt-core","gogurt-path-volume-support"] |
| `distribution` | "gogurt-linux-mounted-volume" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/mounted-volume/linux" |
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

- `/boundaries/components/19`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e608e21ea12cffabb5daaa9bb85fd7e3748086bb8036acd780496862be03ab8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-linux-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/linux",
  "role": "reference_component"
}
```
