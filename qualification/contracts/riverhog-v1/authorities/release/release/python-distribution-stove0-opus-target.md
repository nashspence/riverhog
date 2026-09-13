# Python distribution: stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-opus-target:635ab7223d -->

Optional nonnormative Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-918ebcdc79"></a>
| Concern | Contract |
|---|---|
| <a id="s-ba92f7074a"></a>`artifacts` | [{"coordinate": "dist/stove0_opus_target-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_opus_target-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-37117d817d"></a>`channel` | github-release |
| <a id="s-134300db62"></a>`description` | Optional nonnormative Opus target reference for Stove0. |
| <a id="s-1f2d72f1aa"></a>`requires_python` | >=3.12 |
| <a id="s-6854ea60ff"></a>`role` | reference_component |
| <a id="s-05fdc3293d"></a>`source` | reference/stove0/targets/opus/target/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-opus-target](../../../evidence/relationships.md#rn-313a5c450f)

## Governing policies

- <a id="pa-8ae8b648aa"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-opus-target](../../../evidence/sources.md#src-710aa0c3de) — `reference/stove0/targets/opus/target/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-opus-target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fba51a91e038051799101c3295d0e2be4b2656a955ba98885389cc92a380af28 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_opus_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_opus_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Opus target reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/opus/target/pyproject.toml"
}
```
