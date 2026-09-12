# stove0-review-sampler-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-support:stove0-review-sampler-support-component-boundary:bb2a2bb4c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-f938388ab4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-206b0f259e"></a>
| Field | Shape |
|---|---|
| <a id="s-2c56a708d1"></a>`console_scripts` | additional keys=`stove0-review-sampler-conformance`, `stove0-review-sampler-schemas` |
| <a id="s-81cdf2e045"></a>`dependencies` | ["http-api-contracts","stove0-review-sampler-client","stove0-review-sampler-protocol"] |
| <a id="s-839d7d5e4f"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-11b4d44a4a"></a>`optional_dependencies` | empty object |
| <a id="s-7248067cbd"></a>`path` | "reference/stove0/targets/review/sampler/support" |
| <a id="s-1922397530"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-d2cbd44451"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/68`

### Exact owned JSON

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
