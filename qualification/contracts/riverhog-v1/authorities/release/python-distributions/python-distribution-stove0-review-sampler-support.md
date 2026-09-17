# Python distribution: stove0-review-sampler-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-review-sampler-support:d1e1c18982 -->

Optional nonnormative sampler support for Stove0 review references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-db909caeea"></a>
| Concern | Contract |
|---|---|
| <a id="s-70cf4fedae"></a>`artifacts` | `[{"coordinate":"dist/stove0_review_sampler_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_review_sampler_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-6e0266620f"></a>`channel` | `"github-release"` |
| <a id="s-0a462634c1"></a>`description` | `"Optional nonnormative sampler support for Stove0 review references."` |
| <a id="s-d63b0459f9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-5589a34bd9"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-8d36f3c01e"></a>`publication_identity` | `{"coordinate":"stove0-review-sampler-support","kind":"python-distribution"}` |
| <a id="s-90ac97800a"></a>`requires_python` | `">=3.12"` |
| <a id="s-d808f64d98"></a>`role` | `"reference_component"` |
| <a id="s-65bead379d"></a>`source` | `"reference/stove0/targets/review/sampler/support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-review-sampler-support](../../../evidence/relationships.md#rn-461ec95c05)

## Governing policies

- <a id="pa-318c1e19c4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-487e073b55"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-review-sampler-support](../../../evidence/sources.md#src-08f9b590a6) — [reference/stove0/targets/review/sampler/support/pyproject.toml](../../../../../../reference/stove0/targets/review/sampler/support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-review-sampler-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc5ffc860abe96e9d69286942ef5078437db1af6f8c35d243627a1203460de42 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_review_sampler_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_review_sampler_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative sampler support for Stove0 review references.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-review-sampler-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/stove0/targets/review/sampler/support/pyproject.toml"
}
```

</details>
