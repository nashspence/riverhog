# riverhog-age component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-age:riverhog-age-component-boundary:9702b630dd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-age` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/3`

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
  "dependencies": [],
  "distribution": "riverhog-age",
  "optional_dependencies": {},
  "path": "packages/riverhog-age",
  "role": "reusable_library"
}
```
