# Python distribution: gogurt-core

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-core:9cd8b94580 -->

Portable Gogurt marker, routing, action, and watch semantics.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-d12fd7d1d5"></a>
| Concern | Contract |
|---|---|
| <a id="s-4ea058acac"></a>`artifacts` | `[{"coordinate":"dist/gogurt_core-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt_core-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-c6752b84c5"></a>`channel` | `"github-release"` |
| <a id="s-90f8392ab0"></a>`description` | `"Portable Gogurt marker, routing, action, and watch semantics."` |
| <a id="s-01fc8a4450"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-402e491a5b"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-7396d330e7"></a>`publication_identity` | `{"coordinate":"gogurt-core","kind":"python-distribution"}` |
| <a id="s-1a41c5d00c"></a>`requires_python` | `">=3.12"` |
| <a id="s-01b66a1a3a"></a>`role` | `"reusable_library"` |
| <a id="s-7f695858d5"></a>`source` | `"reference/gogurt/packages/core/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-core](../../../evidence/relationships/nodes.md#rn-e1d2ac7df3)

## Governing policies

- <a id="pa-0141c47efe"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-bd3af509a1"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt-core](../../../evidence/sources/authorities.md#src-2850fdf46b) — [reference/gogurt/packages/core/pyproject.toml](../../../../../../reference/gogurt/packages/core/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-core`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd96ab5e76adbb11e3cab4255a5acebccd5405ead67acbd7b562832f2f37ea35 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_core-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_core-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable Gogurt marker, routing, action, and watch semantics.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-core",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/gogurt/packages/core/pyproject.toml"
}
```

</details>
