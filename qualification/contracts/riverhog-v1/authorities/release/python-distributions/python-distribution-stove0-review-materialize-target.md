# Python distribution: stove0-review-materialize-target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-materialize-target:2c21663b4a -->

Optional nonnormative review materialization target reference for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ccf545d02a"></a>
| Concern | Contract |
|---|---|
| <a id="s-b174ed6dd6"></a>`artifacts` | [{"coordinate": "dist/stove0_review_materialize_target-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_materialize_target-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-c1910f8584"></a>`channel` | github-release |
| <a id="s-b49f3d3405"></a>`description` | Optional nonnormative review materialization target reference for Stove0. |
| <a id="s-bf6a676419"></a>`license_baseline` | first-v1-publication |
| <a id="s-aa634b5656"></a>`license_expression` | CAL-1.0 |
| <a id="s-fb057e5cd5"></a>`publication_identity` | {"coordinate": "stove0-review-materialize-target", "kind": "python-distribution"} |
| <a id="s-370468910f"></a>`requires_python` | >=3.12 |
| <a id="s-33fd4f0f41"></a>`role` | reference_component |
| <a id="s-0eb907755d"></a>`source` | reference/stove0/targets/review/materialize-target/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-materialize-target](../../../evidence/relationships.md#rn-4086d947a3)

## Governing policies

- <a id="pa-e7f027a629"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8602dce331"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-materialize-target](../../../evidence/sources.md#src-8b1c7b0fd5) — `reference/stove0/targets/review/materialize-target/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-materialize-target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ce7cfdc40488fa5d15a070c6aa6e29505e298c8d6e25a3fd4a78b48cacca851 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_materialize_target-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_materialize_target-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative review materialization target reference for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "stove0-review-materialize-target",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/materialize-target/pyproject.toml"
}
```

</details>
