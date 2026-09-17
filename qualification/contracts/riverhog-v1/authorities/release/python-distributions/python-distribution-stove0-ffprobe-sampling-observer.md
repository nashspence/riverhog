# Python distribution: stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-ffprobe-sampling-observer:1f812f3ee0 -->

Optional nonnormative FFprobe sampling-observer reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-31397aa2c1"></a>
| Concern | Contract |
|---|---|
| <a id="s-8187bb8eed"></a>`artifacts` | `[{"coordinate":"dist/stove0_ffprobe_sampling_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_ffprobe_sampling_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-0c6913c2d1"></a>`channel` | `"github-release"` |
| <a id="s-690ff01e16"></a>`description` | `"Optional nonnormative FFprobe sampling-observer reference for Stove0."` |
| <a id="s-5db5aaa0bd"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-a563234fca"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-1e859158ec"></a>`publication_identity` | `{"coordinate":"stove0-ffprobe-sampling-observer","kind":"python-distribution"}` |
| <a id="s-204b24b0b3"></a>`requires_python` | `">=3.12"` |
| <a id="s-f98baf5fda"></a>`role` | `"reference_component"` |
| <a id="s-9277253bc1"></a>`source` | `"reference/stove0/observers/ffprobe-sampling/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-ffprobe-sampling-observer](../../../evidence/relationships/nodes.md#rn-219fad9b72)

## Governing policies

- <a id="pa-b0d6d27173"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-4a30a31d82"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-ffprobe-sampling-observer](../../../evidence/sources/authorities.md#src-e01a1e596d) — [reference/stove0/observers/ffprobe-sampling/pyproject.toml](../../../../../../reference/stove0/observers/ffprobe-sampling/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-ffprobe-sampling-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c4df3c636ec8201e9efad30d4664fa9237f4b9a18d8a6486b2c8143cf58a74b -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-ffprobe-sampling-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/observers/ffprobe-sampling/pyproject.toml"
}
```

</details>
