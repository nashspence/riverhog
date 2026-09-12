# riverhog-provenance-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-contracts:riverhog-provenance-contracts-component-boundary:6aca73c088 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/9`

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
  "distribution": "riverhog-provenance-contracts",
  "optional_dependencies": {},
  "path": "packages/riverhog-provenance-contracts",
  "role": "reusable_library"
}
```
