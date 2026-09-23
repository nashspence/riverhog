# Python distribution: review0-target-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-review0-target-lib:a18042df66 -->

Shared runtime support for Review0 targets.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-8aed062488"></a>
| Concern | Contract |
|---|---|
| <a id="s-79abdf460e"></a>`artifacts` | `[{"coordinate":"dist/review0_target_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/review0_target_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b23a972888"></a>`channel` | `"github-release"` |
| <a id="s-4787457215"></a>`description` | `"Shared runtime support for Review0 targets."` |
| <a id="s-38f00c0d1f"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-e5086efb0e"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-394351ee41"></a>`publication_identity` | `{"coordinate":"review0-target-lib","kind":"python-distribution"}` |
| <a id="s-bfb5a01773"></a>`requires_python` | `">=3.12"` |
| <a id="s-171209da61"></a>`role` | `"reusable_library"` |
| <a id="s-282f78c054"></a>`source` | `"some-implementations/stove0/review0/support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [review0-target-lib](../../../evidence/relationships/nodes.md#rn-88583fbcf2)

## Governing policies

- <a id="pa-97fd7fbcb4"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-82ee60b893"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:review0-target-lib](../../../evidence/sources/authorities.md#src-8039374d09) — [some-implementations/stove0/review0/support/pyproject.toml](../../../../../../some-implementations/stove0/review0/support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/review0-target-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c5008ce07b4dd8b6f9c9d71af7738c36d9d6ff9dcfb36a78bc8cbaf92bcc54ef -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/review0_target_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/review0_target_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Shared runtime support for Review0 targets.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "review0-target-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/review0/support/pyproject.toml"
}
```

</details>
