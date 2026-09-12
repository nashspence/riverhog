# gogurt-listener-runtime component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-runtime-component-boundary:1a2ab8323b -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener-runtime` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/24`

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
    "config-validation",
    "gogurt-core"
  ],
  "distribution": "gogurt-listener-runtime",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/listener-runtime",
  "role": "reusable_library"
}
```
