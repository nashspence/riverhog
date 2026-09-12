# gogurt-macos-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-listener-host:gogurt-macos-listener-host-component-boundary:6f9cd3cc91 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-macos-listener-host` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/17`

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
  "distribution": "gogurt-macos-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/macos",
  "role": "reference_component"
}
```
