# Python distribution: stove0-review-sampler-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-sampler-client:6c0a14ef7a -->

Optional nonnormative sampler-client reference for the Stove0 review target.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-c4952800eb"></a>
| Concern | Contract |
|---|---|
| <a id="s-9ea666029b"></a>`artifacts` | [{"coordinate": "dist/stove0_review_sampler_client-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_sampler_client-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-a3dbc7daeb"></a>`channel` | github-release |
| <a id="s-e9d391f834"></a>`description` | Optional nonnormative sampler-client reference for the Stove0 review target. |
| <a id="s-fe7ae2ca50"></a>`license_baseline` | first-v1-publication |
| <a id="s-154119967b"></a>`license_expression` | Apache-2.0 |
| <a id="s-f65543e4c4"></a>`publication_identity` | {"coordinate": "stove0-review-sampler-client", "kind": "python-distribution"} |
| <a id="s-36873475c8"></a>`requires_python` | >=3.12 |
| <a id="s-10320fd5e0"></a>`role` | reference_component |
| <a id="s-8da7f87f6a"></a>`source` | reference/stove0/targets/review/sampler/client/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-sampler-client](../../../evidence/relationships.md#rn-9c79fa8233)

## Governing policies

- <a id="pa-41b0eb0399"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-de81e6ae44"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-sampler-client](../../../evidence/sources.md#src-9090dabce1) — `reference/stove0/targets/review/sampler/client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-sampler-client`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 21943a9bfbf548007208300bf200398e0e242c6aef8bea248d4b12cb727ae4c4 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_sampler_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_sampler_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative sampler-client reference for the Stove0 review target.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-review-sampler-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/sampler/client/pyproject.toml"
}
```
