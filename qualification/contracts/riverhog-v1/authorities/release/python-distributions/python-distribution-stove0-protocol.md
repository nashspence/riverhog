# Python distribution: stove0-protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-protocol:9ec91cedb1 -->

Canonical content-opaque collection orchestration contracts for stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-21ab1d34d4"></a>
| Concern | Contract |
|---|---|
| <a id="s-a1b4ed2273"></a>`artifacts` | `[{"coordinate":"dist/stove0_protocol-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_protocol-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-93af748a18"></a>`channel` | `"github-release"` |
| <a id="s-a3cca36246"></a>`description` | `"Canonical content-opaque collection orchestration contracts for stove0."` |
| <a id="s-d24cd65ed6"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-364b5d7f5d"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-ee9ee04f1d"></a>`publication_identity` | `{"coordinate":"stove0-protocol","kind":"python-distribution"}` |
| <a id="s-f6af3c6655"></a>`requires_python` | `">=3.12"` |
| <a id="s-7da1529b8a"></a>`role` | `"reusable_library"` |
| <a id="s-69e1df2c1a"></a>`source` | `"reference/stove0/packages/protocol/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-protocol](../../../evidence/relationships.md#rn-72232b7fce)

## Governing policies

- <a id="pa-472f6e6e46"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-40d329b4b7"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-protocol](../../../evidence/sources.md#src-8793f1ad67) — `reference/stove0/packages/protocol/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-protocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbad744fc142c49d5840e2242d3eeeb015b47161de22ea39a69ab8e8f4e92c1b -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_protocol-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_protocol-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Canonical content-opaque collection orchestration contracts for stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-protocol",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/protocol/pyproject.toml"
}
```

</details>
