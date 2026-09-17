# Python distribution: stove0-review-rclone-effect-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-rclone-325cc0d7a3:b082681e6b -->

Optional nonnormative rclone review-effect target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-8aa90656f1"></a>
| Concern | Contract |
|---|---|
| <a id="s-cf29c5755e"></a>`artifacts` | `[{"coordinate":"dist/stove0_review_rclone_effect_target-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_review_rclone_effect_target-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-8a9146ed4a"></a>`channel` | `"github-release"` |
| <a id="s-9bb07dd164"></a>`description` | `"Optional nonnormative rclone review-effect target reference for Stove0."` |
| <a id="s-c52627562c"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-8b41642661"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-7b1d7fbf49"></a>`publication_identity` | `{"coordinate":"stove0-review-rclone-effect-target","kind":"python-distribution"}` |
| <a id="s-9fca961eac"></a>`requires_python` | `">=3.12"` |
| <a id="s-eea589affd"></a>`role` | `"reference_component"` |
| <a id="s-7a71b4ede4"></a>`source` | `"reference/stove0/targets/review/rclone-effect-target/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-rclone-effect-target](../../../evidence/relationships/nodes.md#rn-87283d63df)

## Governing policies

- <a id="pa-45504600a8"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-14aaaf7a94"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-review-rclone-effect-target](../../../evidence/sources/authorities.md#src-e1595567d0) — [reference/stove0/targets/review/rclone-effect-target/pyproject.toml](../../../../../../reference/stove0/targets/review/rclone-effect-target/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-rclone-effect-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9770e71fd7e527cb5b4c41d621dea39b293f8fe73013acfa700687df4255aace -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-review-rclone-effect-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/rclone-effect-target/pyproject.toml"
}
```

</details>
