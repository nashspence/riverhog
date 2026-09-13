# Python distribution: stove0-recipe-config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-recipe-config:73f979ff24 -->

Portable deployment-owned Stove0 recipe catalog contracts and validation.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-14b2bf89f4"></a>
| Concern | Contract |
|---|---|
| <a id="s-53bafee725"></a>`artifacts` | [{"coordinate": "dist/stove0_recipe_config-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_recipe_config-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-b1e45aa9f9"></a>`channel` | github-release |
| <a id="s-5a742dbacc"></a>`description` | Portable deployment-owned Stove0 recipe catalog contracts and validation. |
| <a id="s-20b053f119"></a>`requires_python` | >=3.12 |
| <a id="s-37ea58adfd"></a>`role` | reusable_library |
| <a id="s-8ce4670477"></a>`source` | reference/stove0/packages/recipe-config/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-recipe-config](../../../evidence/relationships.md#rn-1f5664e176)

## Governing policies

- <a id="pa-39528c0202"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-recipe-config](../../../evidence/sources.md#src-124da7769c) — `reference/stove0/packages/recipe-config/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-recipe-config`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb36726e9e1d5fd6208bb192fc2cb817d6388e61cf727c6baa7c59f125082bb3 -->

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
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/recipe-config/pyproject.toml"
}
```
