# Python distribution: stove0-review-target-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-target-support:fcc00bdd69 -->

Optional nonnormative shared review-target support reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-08e669ce75"></a>
| Concern | Contract |
|---|---|
| <a id="s-dd013e2bf2"></a>`artifacts` | [{"coordinate": "dist/stove0_review_target_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_target_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-28d660939d"></a>`channel` | github-release |
| <a id="s-fd55cdcda9"></a>`description` | Optional nonnormative shared review-target support reference for Stove0. |
| <a id="s-21962f95ab"></a>`requires_python` | >=3.12 |
| <a id="s-9dab1fe0d1"></a>`role` | reference_component |
| <a id="s-301fb2b00f"></a>`source` | reference/stove0/targets/review/support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-target-support](../../../evidence/relationships.md#rn-5e88db47d6)

## Governing policies

- <a id="pa-69559c98c9"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-f2c6996797"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-target-support](../../../evidence/sources.md#src-56e42e89a6) — `reference/stove0/targets/review/support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-target-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d00311933f19ce9e07e8f7ef26f2f724ad0e92ae0257079c6444ea5f0fc99d0 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_target_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_target_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative shared review-target support reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/support/pyproject.toml"
}
```
