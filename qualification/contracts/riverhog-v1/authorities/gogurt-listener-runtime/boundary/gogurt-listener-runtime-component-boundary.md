# gogurt-listener-runtime component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-runtime-component-boundary:1a2ab8323b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener-runtime` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["config-validation","gogurt-core"] |
| `distribution` | "gogurt-listener-runtime" |
| `optional_dependencies` | empty object |
| `path` | "reference/gogurt/packages/listener-runtime" |
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

- `/boundaries/components/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f32be8f7fbf756f1e2c5bdf891275e0852e3c691a6c5b7fad745e19a4f404b6 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "gogurt-core"
  ],
  "distribution": "gogurt-listener-runtime",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/listener-runtime",
  "role": "reusable_library"
}
```
