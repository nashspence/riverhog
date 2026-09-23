# Python distribution: a-stove0-ffprobe-sampling-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-ffprobe-samp-e5c85d877f:80e8186b70 -->

FFprobe media sampling observer for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-64d02b4ae9"></a>
| Concern | Contract |
|---|---|
| <a id="s-f1989306d4"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_ffprobe_sampling_observer-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_ffprobe_sampling_observer-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-7e1a125030"></a>`channel` | `"github-release"` |
| <a id="s-cc641a09bd"></a>`description` | `"FFprobe media sampling observer for Stove0."` |
| <a id="s-e6bb24c2d9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-f0dc068d6e"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-918a4fe64a"></a>`publication_identity` | `{"coordinate":"a-stove0-ffprobe-sampling-observer","kind":"python-distribution"}` |
| <a id="s-4d253c12dc"></a>`requires_python` | `">=3.12"` |
| <a id="s-b3c840f253"></a>`role` | `"component"` |
| <a id="s-01f9b5eb4c"></a>`source` | `"some-implementations/stove0/observers/ffprobe-sampling/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-ffprobe-sampling-observer](../../../evidence/relationships/nodes.md#rn-cacf6cd019)

## Governing policies

- <a id="pa-182b6dab47"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-cab27cf985"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-ffprobe-sampling-observer](../../../evidence/sources/authorities.md#src-4672fa9df9) — [some-implementations/stove0/observers/ffprobe-sampling/pyproject.toml](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-ffprobe-sampling-observer`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d4918be8a9e9e1355a12fd7c4a607db3ca15be954c5dee76d6f546ca7ccf95f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_ffprobe_sampling_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_ffprobe_sampling_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "FFprobe media sampling observer for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-stove0-ffprobe-sampling-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/observers/ffprobe-sampling/pyproject.toml"
}
```

</details>
