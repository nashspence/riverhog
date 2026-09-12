# stove0-review-sampler-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-support:stove0-review-sampler-support-component-boundary:bb2a2bb4c0 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/68`

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
| `console_scripts` | object (2 fields) |
| `dependencies` | array (3 items) |
| `distribution` | "stove0-review-sampler-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/stove0/targets/review/sampler/support" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8595c5a14798867e388749752f0379c7ea17080be9e609bf20974c407d8c1800 -->

```json
{
  "console_scripts": {
    "stove0-review-sampler-conformance": "stove0_review_sampler_support.conformance:main",
    "stove0-review-sampler-schemas": "stove0_review_sampler_support.schemas:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-review-sampler-client",
    "stove0-review-sampler-protocol"
  ],
  "distribution": "stove0-review-sampler-support",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/support",
  "role": "reference_component"
}
```
