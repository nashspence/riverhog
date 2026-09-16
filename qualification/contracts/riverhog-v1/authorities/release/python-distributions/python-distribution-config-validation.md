# Python distribution: config-validation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-config-validation:40f4eba7c2 -->

Strict YAML and JSON Schema configuration validation.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1eb7d40fac"></a>
| Concern | Contract |
|---|---|
| <a id="s-1ef889f0ab"></a>`artifacts` | `[{"coordinate":"dist/config_validation-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/config_validation-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-b7635a3241"></a>`channel` | `"github-release"` |
| <a id="s-ddf484653b"></a>`description` | `"Strict YAML and JSON Schema configuration validation."` |
| <a id="s-328f8175a9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-ae9090d303"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-f759eff5de"></a>`publication_identity` | `{"coordinate":"config-validation","kind":"python-distribution"}` |
| <a id="s-9980af7c67"></a>`requires_python` | `">=3.12"` |
| <a id="s-d4cc64f222"></a>`role` | `"internal_build_unit"` |
| <a id="s-257971f953"></a>`source` | `"packages/config-validation/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [config-validation](../../../evidence/relationships.md#rn-b37402f43f)

## Governing policies

- <a id="pa-084ad6086f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ab0d553a83"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:config-validation](../../../evidence/sources.md#src-391296b020) — `packages/config-validation/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/config-validation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3e4f3534163dec0597599694fa4122ad033ebf639e7df82eea77b879f807297 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/config_validation-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/config_validation-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Strict YAML and JSON Schema configuration validation.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "config-validation",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "internal_build_unit",
  "source": "packages/config-validation/pyproject.toml"
}
```

</details>
