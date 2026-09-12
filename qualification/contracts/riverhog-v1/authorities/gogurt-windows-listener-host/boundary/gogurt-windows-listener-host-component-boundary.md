# gogurt-windows-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-listener-host:gogurt-windows-listener-host-component-boundary:ec24a040b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-windows-listener-host` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["gogurt-listener-runtime"] |
| `distribution` | "gogurt-windows-listener-host" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/listener-host/windows" |
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

- `/boundaries/components/18`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8441da71f35f095e333b012c26c2604449eccde2e609b6a64717285fccd15e14 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-windows-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/windows",
  "role": "reference_component"
}
```
