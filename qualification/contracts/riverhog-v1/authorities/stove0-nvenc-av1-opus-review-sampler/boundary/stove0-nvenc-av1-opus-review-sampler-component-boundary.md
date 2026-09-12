# stove0-nvenc-av1-opus-review-sampler component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-comp-fb88459b5d:3827a0fe2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-nvenc-av1-opus-review-sampler` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-nvenc-av1-opus-review-sampler` |
| `dependencies` | ["http-api-contracts","stove0-media-archive-target-contracts","stove0-review-sampler-protocol","stove0-review-sampler-support","stove0-review-target-contracts"] |
| `distribution` | "stove0-nvenc-av1-opus-review-sampler" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/nvenc-av1-opus/review-sampler" |
| `role` | "reference_component" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/components/58`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 684edd07e46116841f1de8146f9f808f2dd255e3685b182a7f4077e56df2f18e -->

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
