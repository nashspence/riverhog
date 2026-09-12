# gogurt-core component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-core-component-boundary:c502c632b9 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-core` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/23`

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
    "config-validation"
  ],
  "distribution": "gogurt-core",
  "optional_dependencies": {},
  "path": "reference/gogurt/packages/core",
  "role": "reusable_library"
}
```
