# Python distribution: stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-nvenc-av1-opus-target:c9ecc6235a -->

Optional nonnormative NVENC AV1 and Opus target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-53f145e23e"></a>
| Concern | Contract |
|---|---|
| <a id="s-8ffaa10b18"></a>`artifacts` | [{"coordinate": "dist/stove0_nvenc_av1_opus_target-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_nvenc_av1_opus_target-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-a9871665f4"></a>`channel` | github-release |
| <a id="s-dd614b41da"></a>`description` | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. |
| <a id="s-8bb3a7af50"></a>`requires_python` | >=3.12 |
| <a id="s-1f2fd4387d"></a>`role` | reference_component |
| <a id="s-0a45edb9f8"></a>`source` | reference/stove0/targets/nvenc-av1-opus/target/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-nvenc-av1-opus-target](../../../evidence/relationships.md#rn-e9196fec57)

## Governing policies

- <a id="pa-82b32d2779"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-nvenc-av1-opus-target](../../../evidence/sources.md#src-b520cbce2a) — `reference/stove0/targets/nvenc-av1-opus/target/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-nvenc-av1-opus-target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af92c0325edae91ed3368b43ccf57341c541a17f3261c0aabf7ea65fcc69b4d7 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative NVENC AV1 and Opus target reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/nvenc-av1-opus/target/pyproject.toml"
}
```
