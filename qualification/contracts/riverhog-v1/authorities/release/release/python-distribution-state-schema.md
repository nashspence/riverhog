# Python distribution: state-schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-state-schema:779b0e5408 -->

Forward-only relational state schema and migration contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-ff0cdfd4d6"></a>
| Concern | Contract |
|---|---|
| <a id="s-e95d6d962d"></a>`artifacts` | [{"coordinate": "dist/state_schema-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/state_schema-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-98abc23196"></a>`channel` | github-release |
| <a id="s-32e3fa588f"></a>`description` | Forward-only relational state schema and migration contracts. |
| <a id="s-3751fd2332"></a>`requires_python` | >=3.12 |
| <a id="s-0e4d139f72"></a>`role` | internal_build_unit |
| <a id="s-6594deda87"></a>`source` | packages/state-schema/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [state-schema](../../../evidence/relationships.md#rn-b37a103e5f)

## Governing policies

- <a id="pa-0b66ccacf1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:state-schema](../../../evidence/sources.md#src-07745187f2) — `packages/state-schema/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/state-schema`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05dc7de790b4f1ea3769c24fc62d734b18ae27b1c6901c5423c6cfe4bf4b780d -->

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
  "requires_python": ">=3.12",
  "role": "internal_build_unit",
  "source": "packages/state-schema/pyproject.toml"
}
```
