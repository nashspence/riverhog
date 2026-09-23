# Python distribution: a-review0-nvenc-av1-opus-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-review0-nvenc-av1-opus-sampler:c048f2a446 -->

NVENC AV1 and Opus sampler for Review0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-7ab6770bda"></a>
| Concern | Contract |
|---|---|
| <a id="s-ea1543a6fd"></a>`artifacts` | `[{"coordinate":"dist/a_review0_nvenc_av1_opus_sampler-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_review0_nvenc_av1_opus_sampler-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b2b6949896"></a>`channel` | `"github-release"` |
| <a id="s-7add49e2c8"></a>`description` | `"NVENC AV1 and Opus sampler for Review0."` |
| <a id="s-a08170ec2a"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-9ef841d158"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-a4b5b1967a"></a>`publication_identity` | `{"coordinate":"a-review0-nvenc-av1-opus-sampler","kind":"python-distribution"}` |
| <a id="s-536849ea9a"></a>`requires_python` | `">=3.12"` |
| <a id="s-2aaff6e80a"></a>`role` | `"component"` |
| <a id="s-fc3258400f"></a>`source` | `"some-implementations/stove0/review0/samplers/nvenc-av1-opus/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-nvenc-av1-opus-sampler](../../../evidence/relationships/nodes.md#rn-8138f74fd5)

## Governing policies

- <a id="pa-f89b874f0e"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-8f7c6db3f2"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-review0-nvenc-av1-opus-sampler](../../../evidence/sources/authorities.md#src-fe3944a703) — [some-implementations/stove0/review0/samplers/nvenc-av1-opus/pyproject.toml](../../../../../../some-implementations/stove0/review0/samplers/nvenc-av1-opus/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-review0-nvenc-av1-opus-sampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ca3ee7d144d3be5f2a4f143924f304d7c2e21ed8dd574a66689a0cdc0f8c8db -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_review0_nvenc_av1_opus_sampler-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_review0_nvenc_av1_opus_sampler-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "NVENC AV1 and Opus sampler for Review0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-review0-nvenc-av1-opus-sampler",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/review0/samplers/nvenc-av1-opus/pyproject.toml"
}
```

</details>
