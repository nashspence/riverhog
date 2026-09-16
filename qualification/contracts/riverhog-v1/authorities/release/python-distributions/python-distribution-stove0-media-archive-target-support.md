# Python distribution: stove0-media-archive-target-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-media-archive-b00fdb281d:bb4d5d7e62 -->

Optional nonnormative projection support for Stove0 media-archive references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-aa73befc81"></a>
| Concern | Contract |
|---|---|
| <a id="s-bd29910202"></a>`artifacts` | [{"coordinate": "dist/stove0_media_archive_target_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_media_archive_target_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-22858dde60"></a>`channel` | github-release |
| <a id="s-49b902ddc9"></a>`description` | Optional nonnormative projection support for Stove0 media-archive references. |
| <a id="s-3242049ede"></a>`license_baseline` | first-v1-publication |
| <a id="s-5e0238e823"></a>`license_expression` | Apache-2.0 |
| <a id="s-2e4bca5189"></a>`publication_identity` | {"coordinate": "stove0-media-archive-target-support", "kind": "python-distribution"} |
| <a id="s-3975191073"></a>`requires_python` | >=3.12 |
| <a id="s-15914f1586"></a>`role` | reference_component |
| <a id="s-f6fc831e11"></a>`source` | reference/stove0/targets/media-archive/support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-media-archive-target-support](../../../evidence/relationships.md#rn-581a2a6b70)

## Governing policies

- <a id="pa-ff9a762af7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4ab433ac65"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-media-archive-target-support](../../../evidence/sources.md#src-0d39629f02) — `reference/stove0/targets/media-archive/support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-media-archive-target-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f5eeda825365fad193a8bc1499cef24371fee95c025cce90506e1f3c69e87c84 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_media_archive_target_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_media_archive_target_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative projection support for Stove0 media-archive references.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-media-archive-target-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/media-archive/support/pyproject.toml"
}
```

</details>
