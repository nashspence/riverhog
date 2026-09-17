# Python distribution: stove0-nvenc-av1-opus-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-nvenc-av1-opus-2d42557fe2:427a4a62ad -->

Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-8c0acc6ef0"></a>
| Concern | Contract |
|---|---|
| <a id="s-a2aec6cc8c"></a>`artifacts` | `[{"coordinate":"dist/stove0_nvenc_av1_opus_review_sampler-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_nvenc_av1_opus_review_sampler-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-71ce441847"></a>`channel` | `"github-release"` |
| <a id="s-373137b7a2"></a>`description` | `"Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0."` |
| <a id="s-ef3420f188"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-a6ef1627dd"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-3f8c30599a"></a>`publication_identity` | `{"coordinate":"stove0-nvenc-av1-opus-review-sampler","kind":"python-distribution"}` |
| <a id="s-3fb2173d5b"></a>`requires_python` | `">=3.12"` |
| <a id="s-ef92a3aee9"></a>`role` | `"reference_component"` |
| <a id="s-c8d541ffb0"></a>`source` | `"reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-nvenc-av1-opus-review-sampler](../../../evidence/relationships.md#rn-3d4e1c390a)

## Governing policies

- <a id="pa-20b9c1604b"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ac457d274d"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-nvenc-av1-opus-review-sampler](../../../evidence/sources.md#src-27f096c998) — [reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml](../../../../../../reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-nvenc-av1-opus-review-sampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f3b24895afbe6e53be808259632aaa1bc90ab53aac3e83bb181973e5a9817d0 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_nvenc_av1_opus_review_sampler-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-nvenc-av1-opus-review-sampler",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/nvenc-av1-opus/review-sampler/pyproject.toml"
}
```

</details>
