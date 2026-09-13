# Python distribution: stove0-media-metadata-observer-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-media-metadata-4ad2c5561c:feaa402ec7 -->

Optional nonnormative media-metadata contract reference for Stove0 observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-77fd9220ee"></a>
| Concern | Contract |
|---|---|
| <a id="s-82660dc5c2"></a>`artifacts` | [{"coordinate": "dist/stove0_media_metadata_observer_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_media_metadata_observer_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-0271c0e593"></a>`channel` | github-release |
| <a id="s-1ac349891e"></a>`description` | Optional nonnormative media-metadata contract reference for Stove0 observers. |
| <a id="s-9cf4565a3f"></a>`requires_python` | >=3.12 |
| <a id="s-555d457b4d"></a>`role` | reference_component |
| <a id="s-cc4111ec82"></a>`source` | reference/stove0/observers/contracts/media-metadata/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-media-metadata-observer-contracts](../../../evidence/relationships.md#rn-c671a0c6fd)

## Governing policies

- <a id="pa-ba217c5bc3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d312437cec"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-media-metadata-observer-contracts](../../../evidence/sources.md#src-64ab741526) — `reference/stove0/observers/contracts/media-metadata/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-media-metadata-observer-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c15df305e7aa4beb43b0e1430122b4ff47e2cf6afc5bbddfcbf1fb5b2d701f87 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_media_metadata_observer_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_media_metadata_observer_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative media-metadata contract reference for Stove0 observers.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/contracts/media-metadata/pyproject.toml"
}
```
