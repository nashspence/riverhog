# Python distribution: stove0-media-archive-target-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-media-archive-fef59c8184:fa633b3cec -->

Optional nonnormative media-archive contract reference for Stove0 targets.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-b5c4f9c43b"></a>
| Concern | Contract |
|---|---|
| <a id="s-c582996ecb"></a>`artifacts` | [{"coordinate": "dist/stove0_media_archive_target_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_media_archive_target_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-ff1c0df78a"></a>`channel` | github-release |
| <a id="s-9fe58984e4"></a>`description` | Optional nonnormative media-archive contract reference for Stove0 targets. |
| <a id="s-4cd9f7d6c9"></a>`requires_python` | >=3.12 |
| <a id="s-422e4f33e5"></a>`role` | reference_component |
| <a id="s-b7a495c1ab"></a>`source` | reference/stove0/targets/media-archive/contracts/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-media-archive-target-contracts](../../../evidence/relationships.md#rn-af0472b6c0)

## Governing policies

- <a id="pa-624f61e7e6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-media-archive-target-contracts](../../../evidence/sources.md#src-7f1e7fec9e) — `reference/stove0/targets/media-archive/contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-media-archive-target-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a6a12707ea016833c945f37d39b4f03d3b9a0baeb9d449d03d8ffbae4a0abb3 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_media_archive_target_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_media_archive_target_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative media-archive contract reference for Stove0 targets.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/media-archive/contracts/pyproject.toml"
}
```
