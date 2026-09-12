# stove0-opus-review-sampler component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-opus-review-sampler:stove0-opus-review-sampler-component-boundary:9d75573a66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1e0b2e41496d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c4e900797cd6"></a>
| Field | Shape |
|---|---|
| <a id="s-4e759ecc1c3c"></a>`console_scripts` | additional keys=`stove0-opus-review-sampler` |
| <a id="s-852717a58626"></a>`dependencies` | ["http-api-contracts","stove0-media-archive-target-contracts","stove0-review-sampler-protocol","stove0-review-sampler-support","stove0-review-target-contracts"] |
| <a id="s-9af7274a36a0"></a>`distribution` | "stove0-opus-review-sampler" |
| <a id="s-672635ee08d8"></a>`optional_dependencies` | empty object |
| <a id="s-29453d4fe765"></a>`path` | "reference/stove0/targets/opus/review-sampler" |
| <a id="s-f842c35b0445"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-6e8e29370291"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/60`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45dedda654ab59cb23ee0afb6886562fecf9295e829d2372c5a077049d9224f5 -->

```json
{
  "console_scripts": {
    "stove0-opus-review-sampler": "stove0_opus_review_sampler.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-archive-target-contracts",
    "stove0-review-sampler-protocol",
    "stove0-review-sampler-support",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-opus-review-sampler",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/opus/review-sampler",
  "role": "reference_component"
}
```
