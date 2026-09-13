# Python distribution: stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-ffprobe-sampling-observer:8a71cdf03b -->

Optional nonnormative FFprobe sampling-observer reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-31397aa2c1"></a>
| Concern | Contract |
|---|---|
| <a id="s-8187bb8eed"></a>`artifacts` | [{"coordinate": "dist/stove0_ffprobe_sampling_observer-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_ffprobe_sampling_observer-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-0c6913c2d1"></a>`channel` | github-release |
| <a id="s-690ff01e16"></a>`description` | Optional nonnormative FFprobe sampling-observer reference for Stove0. |
| <a id="s-204b24b0b3"></a>`requires_python` | >=3.12 |
| <a id="s-f98baf5fda"></a>`role` | reference_component |
| <a id="s-9277253bc1"></a>`source` | reference/stove0/observers/ffprobe-sampling/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-ffprobe-sampling-observer](../../../evidence/relationships.md#rn-219fad9b72)

## Governing policies

- <a id="pa-4dcd402ce5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-ffprobe-sampling-observer](../../../evidence/sources.md#src-e01a1e596d) — `reference/stove0/observers/ffprobe-sampling/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-ffprobe-sampling-observer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d8d598de202520f4b7a39bef46e96173089be2b4e16ef810ad2f802c4a44bd6 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_ffprobe_sampling_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_ffprobe_sampling_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative FFprobe sampling-observer reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/ffprobe-sampling/pyproject.toml"
}
```
