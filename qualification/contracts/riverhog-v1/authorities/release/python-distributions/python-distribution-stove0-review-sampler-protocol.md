# Python distribution: stove0-review-sampler-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-sampler-protocol:c60379f065 -->

Optional nonnormative sampler-protocol reference for the Stove0 review target.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-2afbaa2baf"></a>
| Concern | Contract |
|---|---|
| <a id="s-4a8e493e03"></a>`artifacts` | [{"coordinate": "dist/stove0_review_sampler_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_sampler_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-224943edcd"></a>`channel` | github-release |
| <a id="s-626f8b4721"></a>`description` | Optional nonnormative sampler-protocol reference for the Stove0 review target. |
| <a id="s-ddd10fc988"></a>`license_baseline` | first-v1-publication |
| <a id="s-7530168627"></a>`license_expression` | Apache-2.0 |
| <a id="s-b211febbee"></a>`publication_identity` | {"coordinate": "stove0-review-sampler-protocol", "kind": "python-distribution"} |
| <a id="s-3cfca2bd52"></a>`requires_python` | >=3.12 |
| <a id="s-a8475cdd33"></a>`role` | reference_component |
| <a id="s-ef7f12f4b5"></a>`source` | reference/stove0/targets/review/sampler/protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-sampler-protocol](../../../evidence/relationships.md#rn-eedc3ab10b)

## Governing policies

- <a id="pa-a9b2c29dbf"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a74778b7c4"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-sampler-protocol](../../../evidence/sources.md#src-248793bf47) — `reference/stove0/targets/review/sampler/protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-sampler-protocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc5f37b3e1105220af0f0a8bf9dbcc69f29f914eb39b7cb30d604af5803b1bb7 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_sampler_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_sampler_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative sampler-protocol reference for the Stove0 review target.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-review-sampler-protocol",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/sampler/protocol/pyproject.toml"
}
```

</details>
