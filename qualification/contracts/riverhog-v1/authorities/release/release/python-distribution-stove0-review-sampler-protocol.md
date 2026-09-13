# Python distribution: stove0-review-sampler-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-review-sampler-protocol:a6cd35db00 -->

Optional nonnormative sampler-protocol reference for the Stove0 review target.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-2afbaa2baf"></a>
| Concern | Contract |
|---|---|
| <a id="s-4a8e493e03"></a>`artifacts` | [{"coordinate": "dist/stove0_review_sampler_protocol-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_sampler_protocol-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-224943edcd"></a>`channel` | github-release |
| <a id="s-626f8b4721"></a>`description` | Optional nonnormative sampler-protocol reference for the Stove0 review target. |
| <a id="s-3cfca2bd52"></a>`requires_python` | >=3.12 |
| <a id="s-a8475cdd33"></a>`role` | reference_component |
| <a id="s-ef7f12f4b5"></a>`source` | reference/stove0/targets/review/sampler/protocol/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-sampler-protocol](../../../evidence/relationships.md#rn-eedc3ab10b)

## Governing policies

- <a id="pa-440d701d8c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1756e67d44454db59752961d588e8319809c6fec3592c1961fec585940369eb7 -->

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
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/sampler/protocol/pyproject.toml"
}
```
