# Python distribution: state-schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-state-schema:5356eda8be -->

Forward-only relational state schema and migration contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ff0cdfd4d6"></a>
| Concern | Contract |
|---|---|
| <a id="s-e95d6d962d"></a>`artifacts` | `[{"coordinate":"dist/state_schema-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/state_schema-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-98abc23196"></a>`channel` | `"github-release"` |
| <a id="s-32e3fa588f"></a>`description` | `"Forward-only relational state schema and migration contracts."` |
| <a id="s-3bbb63e7ee"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-d41b2869c4"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2011303347"></a>`publication_identity` | `{"coordinate":"state-schema","kind":"python-distribution"}` |
| <a id="s-3751fd2332"></a>`requires_python` | `">=3.12"` |
| <a id="s-0e4d139f72"></a>`role` | `"internal_build_unit"` |
| <a id="s-6594deda87"></a>`source` | `"packages/state-schema/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [state-schema](../../../evidence/relationships.md#rn-b37a103e5f)

## Governing policies

- <a id="pa-842dcb9a71"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-64a719318d"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:state-schema](../../../evidence/sources.md#src-07745187f2) — [packages/state-schema/pyproject.toml](../../../../../../packages/state-schema/pyproject.toml)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/state-schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b3c862feb80db2b11d99b7b41626643b1bc3873147b30dd1ee055df4d1ea556 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/state_schema-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/state_schema-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Forward-only relational state schema and migration contracts.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "state-schema",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "internal_build_unit",
  "source": "packages/state-schema/pyproject.toml"
}
```

</details>
