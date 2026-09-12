# stove0-review-materialize-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-materialize-target:stove0-review-materialize-target-component-boundary:921c3e7a46 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-materialize-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/63`

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
  "console_scripts": {
    "stove0-review-materialize-target": "stove0_review_materialize_target.app:main"
  },
  "dependencies": [
    "riverhog-client",
    "stove0-review-target-contracts",
    "stove0-review-target-support",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-materialize-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/materialize-target",
  "role": "reference_component"
}
```
