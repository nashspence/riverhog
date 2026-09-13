# Python distribution: stove0-review-sampler-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-review-sampler-support:1115f51252 -->

Optional nonnormative sampler support for Stove0 review references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-db909caeea"></a>
| Concern | Contract |
|---|---|
| <a id="s-70cf4fedae"></a>`artifacts` | [{"coordinate": "dist/stove0_review_sampler_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_sampler_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-6e0266620f"></a>`channel` | github-release |
| <a id="s-0a462634c1"></a>`description` | Optional nonnormative sampler support for Stove0 review references. |
| <a id="s-90ac97800a"></a>`requires_python` | >=3.12 |
| <a id="s-d808f64d98"></a>`role` | reference_component |
| <a id="s-65bead379d"></a>`source` | reference/stove0/targets/review/sampler/support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-sampler-support](../../../evidence/relationships.md#rn-461ec95c05)

## Governing policies

- <a id="pa-871427a012"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-sampler-support](../../../evidence/sources.md#src-08f9b590a6) — `reference/stove0/targets/review/sampler/support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-sampler-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1433a0b0317cc766841c033fc19870f416f6ac6f1ed3e12d5bed10ba16e47c3 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_sampler_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_sampler_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative sampler support for Stove0 review references.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/sampler/support/pyproject.toml"
}
```
