# Python distribution: stove0-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-opus-target:c35179f9a7 -->

Optional nonnormative Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-918ebcdc79"></a>
| Concern | Contract |
|---|---|
| <a id="s-ba92f7074a"></a>`artifacts` | [{"coordinate": "dist/stove0_opus_target-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_opus_target-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-37117d817d"></a>`channel` | github-release |
| <a id="s-134300db62"></a>`description` | Optional nonnormative Opus target reference for Stove0. |
| <a id="s-da63babcdb"></a>`license_baseline` | first-v1-publication |
| <a id="s-175f1b91cb"></a>`license_expression` | CAL-1.0 |
| <a id="s-e3ec53241d"></a>`publication_identity` | {"coordinate": "stove0-opus-target", "kind": "python-distribution"} |
| <a id="s-1f2d72f1aa"></a>`requires_python` | >=3.12 |
| <a id="s-6854ea60ff"></a>`role` | reference_component |
| <a id="s-05fdc3293d"></a>`source` | reference/stove0/targets/opus/target/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-opus-target](../../../evidence/relationships.md#rn-313a5c450f)

## Governing policies

- <a id="pa-e5fc747cd7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-7a3b8e372f"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32e38c994f96bda04981564a7a7e804af72668724496ca895a81bae3279717a1 -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-opus-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/opus/target/pyproject.toml"
}
```

</details>
