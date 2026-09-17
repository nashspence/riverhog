# Python distribution: gogurt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt:7df28f3cb4 -->

Optional nonnormative mounted-volume ingestion reference application for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-96a0be4c25"></a>
| Concern | Contract |
|---|---|
| <a id="s-9f96bcff2f"></a>`artifacts` | `[{"coordinate":"dist/gogurt-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-9323e67d07"></a>`channel` | `"github-release"` |
| <a id="s-d290b63892"></a>`description` | `"Optional nonnormative mounted-volume ingestion reference application for Riverhog."` |
| <a id="s-1eff1678b9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-6fe04d5edc"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-960ac88e05"></a>`publication_identity` | `{"coordinate":"gogurt","kind":"python-distribution"}` |
| <a id="s-55f6e90fbe"></a>`requires_python` | `">=3.12"` |
| <a id="s-198b438cc4"></a>`role` | `"reference_application"` |
| <a id="s-1962b7fa78"></a>`source` | `"reference/gogurt/application/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt](../../../evidence/relationships/nodes.md#rn-eeb2be4a71)

## Governing policies

- <a id="pa-37d8e79811"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1939d299f8"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt](../../../evidence/sources/authorities.md#src-3e7b582a54) — [reference/gogurt/application/pyproject.toml](../../../../../../reference/gogurt/application/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f999054fbeefdcdef454ab5169ad9b3a446b09209e25f9256c8ea4cba1495be -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative mounted-volume ingestion reference application for Riverhog.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/gogurt/application/pyproject.toml"
}
```

</details>
