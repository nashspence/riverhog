# Python distribution: a-stove0-nvenc-av1-opus-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-nvenc-av1-opus-target:4846b3378d -->

NVENC AV1 and Opus transformation target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-eb8653cad4"></a>
| Concern | Contract |
|---|---|
| <a id="s-548206c618"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_nvenc_av1_opus_target-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_nvenc_av1_opus_target-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-1b6ce594fd"></a>`channel` | `"github-release"` |
| <a id="s-0edb5d2f41"></a>`description` | `"NVENC AV1 and Opus transformation target for Stove0."` |
| <a id="s-c0b3bb32d6"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-45fc50ea69"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-e295c1a0eb"></a>`publication_identity` | `{"coordinate":"a-stove0-nvenc-av1-opus-target","kind":"python-distribution"}` |
| <a id="s-eed52d9043"></a>`requires_python` | `">=3.12"` |
| <a id="s-a17a288b5d"></a>`role` | `"component"` |
| <a id="s-b73cbd2459"></a>`source` | `"some-implementations/stove0/targets/nvenc-av1-opus/target/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-nvenc-av1-opus-target](../../../evidence/relationships/nodes.md#rn-7fd150cf21)

## Governing policies

- <a id="pa-e5469d0f43"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-2a0adc0ace"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-nvenc-av1-opus-target](../../../evidence/sources/authorities.md#src-8a460b4d59) — [some-implementations/stove0/targets/nvenc-av1-opus/target/pyproject.toml](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-nvenc-av1-opus-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 390946e73387ea1df150a53f847cb0ff8fe249558c99195147db795dbd2f29f5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_nvenc_av1_opus_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_nvenc_av1_opus_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "NVENC AV1 and Opus transformation target for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-stove0-nvenc-av1-opus-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/targets/nvenc-av1-opus/target/pyproject.toml"
}
```

</details>
