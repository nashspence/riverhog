# stove0-observer-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-protocol:stove0-observer-protocol-component-boundary:fea970d8a5 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-protocol` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/48`

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
    "stove0-protocol"
  ],
  "distribution": "stove0-observer-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-protocol",
  "role": "reusable_library"
}
```
