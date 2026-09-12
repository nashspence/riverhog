# gogurt-macos-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-mounted-volume:gogurt-macos-mounted-volume-component-boundary:b842309763 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-macos-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/20`

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
  "distribution": "gogurt-macos-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/macos",
  "role": "reference_component"
}
```
