# Python distribution: stove0-review-rclone-effect-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-review-rclone-325cc0d7a3:ca0bf81fb2 -->

Optional nonnormative rclone review-effect target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-8aa90656f1"></a>
| Concern | Contract |
|---|---|
| <a id="s-cf29c5755e"></a>`artifacts` | [{"coordinate": "dist/stove0_review_rclone_effect_target-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_rclone_effect_target-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-8a9146ed4a"></a>`channel` | github-release |
| <a id="s-9bb07dd164"></a>`description` | Optional nonnormative rclone review-effect target reference for Stove0. |
| <a id="s-9fca961eac"></a>`requires_python` | >=3.12 |
| <a id="s-eea589affd"></a>`role` | reference_component |
| <a id="s-7a71b4ede4"></a>`source` | reference/stove0/targets/review/rclone-effect-target/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-rclone-effect-target](../../../evidence/relationships.md#rn-87283d63df)

## Governing policies

- <a id="pa-4d87337c9c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-rclone-effect-target](../../../evidence/sources.md#src-e1595567d0) — `reference/stove0/targets/review/rclone-effect-target/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-rclone-effect-target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f57ca387400dfa433fe1502501c637695d09339f94949dec90ab4c7cc861f295 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_rclone_effect_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_rclone_effect_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative rclone review-effect target reference for Stove0.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/rclone-effect-target/pyproject.toml"
}
```
