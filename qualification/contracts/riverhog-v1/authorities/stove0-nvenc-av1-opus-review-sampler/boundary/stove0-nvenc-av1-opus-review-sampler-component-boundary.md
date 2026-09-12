# stove0-nvenc-av1-opus-review-sampler component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler-comp-fb88459b5d:3827a0fe2c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-review-sampler](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-4ee42bbed893) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f7d14b57f639"></a>
| Field | Shape |
|---|---|
| <a id="s-32f717a756af"></a>`console_scripts` | additional keys=`stove0-nvenc-av1-opus-review-sampler` |
| <a id="s-82d04827b632"></a>`dependencies` | ["http-api-contracts","stove0-media-archive-target-contracts","stove0-review-sampler-protocol","stove0-review-sampler-support","stove0-review-target-contracts"] |
| <a id="s-243dd46476a6"></a>`distribution` | "stove0-nvenc-av1-opus-review-sampler" |
| <a id="s-24f74d56df63"></a>`optional_dependencies` | empty object |
| <a id="s-661357b805d5"></a>`path` | "reference/stove0/targets/nvenc-av1-opus/review-sampler" |
| <a id="s-0eabf0706142"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-70f50645b4da"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
