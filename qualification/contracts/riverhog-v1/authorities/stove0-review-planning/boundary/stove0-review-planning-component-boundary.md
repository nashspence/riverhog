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

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (1 fields) |
| `dependencies` | array (3 items) |
| `distribution` | "stove0-review-planning" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/review/planning" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ebd931b6d8a76f36f030b0228c4681748aa8701e58ea8059fe59e481efdbd3f -->

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
