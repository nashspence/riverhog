# stove0-recipe-config component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-recipe-config:stove0-recipe-config-component-boundary:32ce7b8e00 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-recipe-config` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/52`

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
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-recipe-config",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/recipe-config",
  "role": "reusable_library"
}
```
