# http-api-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:http-api-contracts:http-api-contracts-component-boundary:f712265b1f -->

| Audit field | Value |
|---|---|
| Authority | `http-api-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/1`

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
  "distribution": "http-api-contracts",
  "optional_dependencies": {},
  "path": "packages/http-api-contracts",
  "role": "reusable_library"
}
```
