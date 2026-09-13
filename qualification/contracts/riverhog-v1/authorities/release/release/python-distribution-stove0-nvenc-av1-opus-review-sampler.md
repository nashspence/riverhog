# Python distribution: stove0-nvenc-av1-opus-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-nvenc-av1-opus-2d42557fe2:270edfdd8c -->

Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-8c0acc6ef0"></a>
| Concern | Contract |
|---|---|
| <a id="s-a2aec6cc8c"></a>`artifacts` | [{"coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-71ce441847"></a>`channel` | github-release |
| <a id="s-373137b7a2"></a>`description` | Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0. |
| <a id="s-3fb2173d5b"></a>`requires_python` | >=3.12 |
| <a id="s-ef92a3aee9"></a>`role` | reference_component |
| <a id="s-c8d541ffb0"></a>`source` | reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-nvenc-av1-opus-review-sampler](../../../evidence/relationships.md#rn-3d4e1c390a)

## Governing policies

- <a id="pa-aea9766f95"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-nvenc-av1-opus-review-sampler](../../../evidence/sources.md#src-27f096c998) — `reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-nvenc-av1-opus-review-sampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a1a96bfc97c51942f68e3c480ca21b7d670145191f8d3b768432d25770f2c4a -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml"
}
```
