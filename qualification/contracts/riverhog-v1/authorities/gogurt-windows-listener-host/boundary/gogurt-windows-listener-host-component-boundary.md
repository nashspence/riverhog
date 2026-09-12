# gogurt-windows-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-windows-listener-host:gogurt-windows-listener-host-component-boundary:ec24a040b4 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-windows-listener-host` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/18`

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
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-windows-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/windows",
  "role": "reference_component"
}
```
