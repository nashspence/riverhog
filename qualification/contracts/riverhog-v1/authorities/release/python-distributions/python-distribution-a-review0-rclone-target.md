# Python distribution: a-review0-rclone-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-review0-rclone-target:65cc6ebc74 -->

Review0 rclone delivery target for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-72de871d58"></a>
| Concern | Contract |
|---|---|
| <a id="s-f5fe03f21c"></a>`artifacts` | `[{"coordinate":"dist/a_review0_rclone_target-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_review0_rclone_target-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-ed847dfdba"></a>`channel` | `"github-release"` |
| <a id="s-bf26a41128"></a>`description` | `"Review0 rclone delivery target for Stove0."` |
| <a id="s-5df721dd2c"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-ec16a9063f"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-b9d6857436"></a>`publication_identity` | `{"coordinate":"a-review0-rclone-target","kind":"python-distribution"}` |
| <a id="s-66a00b2952"></a>`requires_python` | `">=3.12"` |
| <a id="s-7ce2e6af53"></a>`role` | `"component"` |
| <a id="s-92291de061"></a>`source` | `"some-implementations/stove0/review0/rclone-effect-target/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-review0-rclone-target](../../../evidence/relationships/nodes.md#rn-b5b7de6707)

## Governing policies

- <a id="pa-1e4cc8a340"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-8571acde6a"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-review0-rclone-target](../../../evidence/sources/authorities.md#src-813f784d9d) — [some-implementations/stove0/review0/rclone-effect-target/pyproject.toml](../../../../../../some-implementations/stove0/review0/rclone-effect-target/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-review0-rclone-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be0a38dd3e7fe2205c3f69a1af70ed76c983442eb4f22c3b66fc9b8ede5be1b4 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_review0_rclone_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_review0_rclone_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Review0 rclone delivery target for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "a-review0-rclone-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/review0/rclone-effect-target/pyproject.toml"
}
```

</details>
