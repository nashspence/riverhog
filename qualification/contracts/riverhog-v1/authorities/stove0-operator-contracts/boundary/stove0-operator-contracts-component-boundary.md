# stove0-operator-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-operator-contracts:stove0-operator-contracts-component-boundary:4b9c2c4e22 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-operator-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/50`

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
    "lifecycle-events",
    "riverhog-protocol",
    "stove0-observer-protocol",
    "stove0-protocol",
    "stove0-recipe-config",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-operator-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/operator-contracts",
  "role": "reusable_library"
}
```
