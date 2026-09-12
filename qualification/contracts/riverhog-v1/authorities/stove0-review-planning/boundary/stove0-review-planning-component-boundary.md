# stove0-review-planning component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-planning:stove0-review-planning-component-boundary:d9df6bda01 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-planning` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/64`

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
    "stove0-review-planning": "stove0_review_planning.conformance:main"
  },
  "dependencies": [
    "stove0-media-sampling-observer-contracts",
    "stove0-protocol",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-review-planning",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/planning",
  "role": "reference_component"
}
```
