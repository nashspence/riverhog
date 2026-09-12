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

## Contract

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
