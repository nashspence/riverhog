# gogurt-windows-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-mounted-volume:gogurt-windows-mounted-volume-component-boundary:45fa244e43 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-windows-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/22`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (0 fields) |
| `dependencies` | array (2 items) |
| `distribution` | "gogurt-windows-mounted-volume" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/mounted-volume/windows" |
| `role` | "reference_component" |

## Complete owned contract

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
