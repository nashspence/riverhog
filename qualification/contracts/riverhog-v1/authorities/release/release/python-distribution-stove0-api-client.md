# Python distribution: stove0-api-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-stove0-api-client:50e47b4307 -->

Official Python client for the stove0 v1 workflow API.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-0bb6612302"></a>
| Concern | Contract |
|---|---|
| <a id="s-a89e3100a3"></a>`artifacts` | [{"coordinate": "dist/stove0_api_client-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/stove0_api_client-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-2d20f67164"></a>`channel` | github-release |
| <a id="s-b9888c2b34"></a>`description` | Official Python client for the stove0 v1 workflow API. |
| <a id="s-4e0677b005"></a>`requires_python` | >=3.12 |
| <a id="s-46dca634a2"></a>`role` | reusable_library |
| <a id="s-21998422fc"></a>`source` | reference/stove0/packages/api-client/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-api-client](../../../evidence/relationships.md#rn-0b595a9ebc)

## Governing policies

- <a id="pa-5a3069226a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-api-client](../../../evidence/sources.md#src-65fbc03822) — `reference/stove0/packages/api-client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-api-client`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efbc22b6136698d160ff388497d455b3d3b65ca66531e306c5debe21ad897359 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_api_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_api_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Official Python client for the stove0 v1 workflow API.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/api-client/pyproject.toml"
}
```
