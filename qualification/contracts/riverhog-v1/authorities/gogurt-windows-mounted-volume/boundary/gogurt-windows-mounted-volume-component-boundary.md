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

## Contract

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
