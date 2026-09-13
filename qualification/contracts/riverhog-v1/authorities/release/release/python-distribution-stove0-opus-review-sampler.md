# Python distribution: stove0-opus-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-opus-review-sampler:3fc337d7e9 -->

Optional nonnormative Opus review-sampler reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-e78cd11326"></a>
| Concern | Contract |
|---|---|
| <a id="s-4573848aee"></a>`artifacts` | [{"coordinate": "dist/stove0_opus_review_sampler-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_opus_review_sampler-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-cd7da95505"></a>`channel` | github-release |
| <a id="s-113256f19d"></a>`description` | Optional nonnormative Opus review-sampler reference for Stove0. |
| <a id="s-8e5e3953d1"></a>`requires_python` | >=3.12 |
| <a id="s-8bc6ec972d"></a>`role` | reference_component |
| <a id="s-6ea5a278a4"></a>`source` | reference/stove0/targets/opus/review-sampler/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-opus-review-sampler](../../../evidence/relationships.md#rn-36756ccd77)

## Governing policies

- <a id="pa-267a8d3b51"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-opus-review-sampler](../../../evidence/sources.md#src-21ddb4cd93) — `reference/stove0/targets/opus/review-sampler/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-opus-review-sampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0eaa1463ed8f1439f2bf35ec5c7af32c60853298e2435d132278b74f51c0245 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_opus_review_sampler-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_opus_review_sampler-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Opus review-sampler reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/opus/review-sampler/pyproject.toml"
}
```
