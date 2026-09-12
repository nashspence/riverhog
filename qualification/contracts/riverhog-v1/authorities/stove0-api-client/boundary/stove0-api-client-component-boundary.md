# stove0-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-api-client:stove0-api-client-component-boundary:0f99add1c2 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-api-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/46`

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
    "http-api-contracts",
    "stove0-operator-contracts",
    "stove0-protocol"
  ],
  "distribution": "stove0-api-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/api-client",
  "role": "reusable_library"
}
```
