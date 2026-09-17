# Python distribution: stove0-recipe-config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-recipe-config:af46f42f53 -->

Portable deployment-owned Stove0 recipe catalog contracts and validation.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-14b2bf89f4"></a>
| Concern | Contract |
|---|---|
| <a id="s-53bafee725"></a>`artifacts` | `[{"coordinate":"dist/stove0_recipe_config-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_recipe_config-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b1e45aa9f9"></a>`channel` | `"github-release"` |
| <a id="s-5a742dbacc"></a>`description` | `"Portable deployment-owned Stove0 recipe catalog contracts and validation."` |
| <a id="s-a7767186ba"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-8600748317"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-0e29a60d96"></a>`publication_identity` | `{"coordinate":"stove0-recipe-config","kind":"python-distribution"}` |
| <a id="s-20b053f119"></a>`requires_python` | `">=3.12"` |
| <a id="s-37ea58adfd"></a>`role` | `"reusable_library"` |
| <a id="s-8ce4670477"></a>`source` | `"reference/stove0/packages/recipe-config/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-recipe-config](../../../evidence/relationships/nodes.md#rn-1f5664e176)

## Governing policies

- <a id="pa-2776083b79"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-5bb3b2a3d8"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-recipe-config](../../../evidence/sources/authorities.md#src-124da7769c) — [reference/stove0/packages/recipe-config/pyproject.toml](../../../../../../reference/stove0/packages/recipe-config/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-recipe-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be1ba4b6ab5677236d35e5ce93705adb1aa181cfad66f7eb727ea06c0aba7fe9 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_recipe_config-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_recipe_config-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable deployment-owned Stove0 recipe catalog contracts and validation.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-recipe-config",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/recipe-config/pyproject.toml"
}
```

</details>
