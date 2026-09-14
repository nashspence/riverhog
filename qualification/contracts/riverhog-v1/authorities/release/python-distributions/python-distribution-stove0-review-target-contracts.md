# Python distribution: stove0-review-target-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-target-contracts:69de5de3ee -->

Optional nonnormative review contract reference for Stove0 targets.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-b3e4d0c558"></a>
| Concern | Contract |
|---|---|
| <a id="s-404ed6e0bb"></a>`artifacts` | [{"coordinate": "dist/stove0_review_target_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_target_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-c189a8c825"></a>`channel` | github-release |
| <a id="s-86ba7ea602"></a>`description` | Optional nonnormative review contract reference for Stove0 targets. |
| <a id="s-02ded21113"></a>`license_baseline` | first-v1-publication |
| <a id="s-9554858586"></a>`license_expression` | Apache-2.0 |
| <a id="s-c668934187"></a>`publication_identity` | {"coordinate": "stove0-review-target-contracts", "kind": "python-distribution"} |
| <a id="s-07f47dc329"></a>`requires_python` | >=3.12 |
| <a id="s-0074a2c5e0"></a>`role` | reference_component |
| <a id="s-4aed6439aa"></a>`source` | reference/stove0/targets/review/contracts/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-target-contracts](../../../evidence/relationships.md#rn-83d36842d1)

## Governing policies

- <a id="pa-ecb20447db"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-d80587e4dd"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-target-contracts](../../../evidence/sources.md#src-288a68ea62) — `reference/stove0/targets/review/contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-target-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 935d665b29c2473ddb81dd512633867df79a861b64997f5d795b5784fa5e5bcf -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_target_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_target_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative review contract reference for Stove0 targets.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-review-target-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/contracts/pyproject.toml"
}
```
