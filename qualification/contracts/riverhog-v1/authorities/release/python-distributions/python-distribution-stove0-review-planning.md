# Python distribution: stove0-review-planning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-planning:fbe1297b67 -->

Optional nonnormative planning bridge for maintained Stove0 review references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-de29e5410b"></a>
| Concern | Contract |
|---|---|
| <a id="s-30ac634e87"></a>`artifacts` | `[{"coordinate":"dist/stove0_review_planning-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_review_planning-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-d351ec69fd"></a>`channel` | `"github-release"` |
| <a id="s-cba7731cc5"></a>`description` | `"Optional nonnormative planning bridge for maintained Stove0 review references."` |
| <a id="s-ec9b009600"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-8b988459f4"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-dada31f521"></a>`publication_identity` | `{"coordinate":"stove0-review-planning","kind":"python-distribution"}` |
| <a id="s-b9d1c72f24"></a>`requires_python` | `">=3.12"` |
| <a id="s-9f59f5c0b8"></a>`role` | `"reference_component"` |
| <a id="s-71fba89045"></a>`source` | `"reference/stove0/targets/review/planning/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-planning](../../../evidence/relationships/nodes.md#rn-dbd324b350)

## Governing policies

- <a id="pa-725683808c"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-d3870c1dcc"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-review-planning](../../../evidence/sources/authorities.md#src-41edb4d9f8) — [reference/stove0/targets/review/planning/pyproject.toml](../../../../../../reference/stove0/targets/review/planning/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-planning`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 412b0e78ba647c60d48beceab89fbce8983eef7820a927f8195f2180af75e893 -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-review-planning",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/planning/pyproject.toml"
}
```

</details>
