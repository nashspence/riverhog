# Python distribution: stove0-media-sampling-observer-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-media-sampling-4ca590c993:65c10085b8 -->

Optional nonnormative media-sampling contract reference for Stove0 observers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-2cba61e601"></a>
| Concern | Contract |
|---|---|
| <a id="s-eb0f414944"></a>`artifacts` | `[{"coordinate":"dist/stove0_media_sampling_observer_contracts-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_media_sampling_observer_contracts-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-746731254e"></a>`channel` | `"github-release"` |
| <a id="s-86f06d9fa2"></a>`description` | `"Optional nonnormative media-sampling contract reference for Stove0 observers."` |
| <a id="s-5120a00b37"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-b99d46021e"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-4c84c5b198"></a>`publication_identity` | `{"coordinate":"stove0-media-sampling-observer-contracts","kind":"python-distribution"}` |
| <a id="s-d12d732533"></a>`requires_python` | `">=3.12"` |
| <a id="s-405f29c76f"></a>`role` | `"reference_component"` |
| <a id="s-6e9d31eb75"></a>`source` | `"reference/stove0/observers/contracts/media-sampling/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-media-sampling-observer-contracts](../../../evidence/relationships.md#rn-8acbd56de7)

## Governing policies

- <a id="pa-7766a85d47"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-44c1ccc1f2"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-media-sampling-observer-contracts](../../../evidence/sources.md#src-135c11f96b) — [reference/stove0/observers/contracts/media-sampling/pyproject.toml](../../../../../../reference/stove0/observers/contracts/media-sampling/pyproject.toml)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-media-sampling-observer-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3894318136e4e040b9ab07aac9bfed1895f7db9c031f54ea4841e23143e0174e -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-media-sampling-observer-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/contracts/media-sampling/pyproject.toml"
}
```

</details>
