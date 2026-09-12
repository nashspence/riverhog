# stove0-nvenc-av1-opus-review-sampler component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-comp-fb88459b5d:3827a0fe2c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-nvenc-av1-opus-review-sampler` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/58`

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
    "stove0-nvenc-av1-opus-review-sampler": "stove0_nvenc_av1_opus_review_sampler.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-archive-target-contracts",
    "stove0-review-sampler-protocol",
    "stove0-review-sampler-support",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-nvenc-av1-opus-review-sampler",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/nvenc-av1-opus/review-sampler",
  "role": "reference_component"
}
```
