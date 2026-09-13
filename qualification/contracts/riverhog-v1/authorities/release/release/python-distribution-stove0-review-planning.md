# Python distribution: stove0-review-planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-review-planning:7fb316f94b -->

Optional nonnormative planning bridge for maintained Stove0 review references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-de29e5410b"></a>
| Concern | Contract |
|---|---|
| <a id="s-30ac634e87"></a>`artifacts` | [{"coordinate": "dist/stove0_review_planning-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_review_planning-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-d351ec69fd"></a>`channel` | github-release |
| <a id="s-cba7731cc5"></a>`description` | Optional nonnormative planning bridge for maintained Stove0 review references. |
| <a id="s-b9d1c72f24"></a>`requires_python` | >=3.12 |
| <a id="s-9f59f5c0b8"></a>`role` | reference_component |
| <a id="s-71fba89045"></a>`source` | reference/stove0/targets/review/planning/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-planning](../../../evidence/relationships.md#rn-dbd324b350)

## Governing policies

- <a id="pa-8d826f7725"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-review-planning](../../../evidence/sources.md#src-41edb4d9f8) — `reference/stove0/targets/review/planning/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-planning`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d14bceab513fd5b9c364eb0f3d1c2f07e02feeffdc4047e75ebf96aea7dc816 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_planning-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_planning-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative planning bridge for maintained Stove0 review references.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/planning/pyproject.toml"
}
```
