# stove0-review-sampler-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-client:stove0-review-sampler-client-component-boundary:2da2ddf525 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-review-sampler-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/66`

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
    "stove0-review-sampler-protocol"
  ],
  "distribution": "stove0-review-sampler-client",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/client",
  "role": "reference_component"
}
```
