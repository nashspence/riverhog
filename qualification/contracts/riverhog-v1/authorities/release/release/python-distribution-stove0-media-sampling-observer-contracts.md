# Python distribution: stove0-media-sampling-observer-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-media-sampling-4ca590c993:bc008a112a -->

Optional nonnormative media-sampling contract reference for Stove0 observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-2cba61e601"></a>
| Concern | Contract |
|---|---|
| <a id="s-eb0f414944"></a>`artifacts` | [{"coordinate": "dist/stove0_media_sampling_observer_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_media_sampling_observer_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-746731254e"></a>`channel` | github-release |
| <a id="s-86f06d9fa2"></a>`description` | Optional nonnormative media-sampling contract reference for Stove0 observers. |
| <a id="s-d12d732533"></a>`requires_python` | >=3.12 |
| <a id="s-405f29c76f"></a>`role` | reference_component |
| <a id="s-6e9d31eb75"></a>`source` | reference/stove0/observers/contracts/media-sampling/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-media-sampling-observer-contracts](../../../evidence/relationships.md#rn-8acbd56de7)

## Governing policies

- <a id="pa-ca3ac10d1b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-media-sampling-observer-contracts](../../../evidence/sources.md#src-135c11f96b) — `reference/stove0/observers/contracts/media-sampling/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-media-sampling-observer-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78eddbe1783d1be7c37b709675c6f81e7e6b0eb4982e274fe5ab2b2c8caa4b5b -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_media_sampling_observer_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_media_sampling_observer_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative media-sampling contract reference for Stove0 observers.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/contracts/media-sampling/pyproject.toml"
}
```
