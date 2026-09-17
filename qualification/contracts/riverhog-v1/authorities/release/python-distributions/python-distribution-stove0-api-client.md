# Python distribution: stove0-api-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-api-client:6a123d166a -->

Official Python client for the stove0 v1 workflow API.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-0bb6612302"></a>
| Concern | Contract |
|---|---|
| <a id="s-a89e3100a3"></a>`artifacts` | `[{"coordinate":"dist/stove0_api_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_api_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-2d20f67164"></a>`channel` | `"github-release"` |
| <a id="s-b9888c2b34"></a>`description` | `"Official Python client for the stove0 v1 workflow API."` |
| <a id="s-63a90b4cf7"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-06e8c84e58"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-18ed427769"></a>`publication_identity` | `{"coordinate":"stove0-api-client","kind":"python-distribution"}` |
| <a id="s-4e0677b005"></a>`requires_python` | `">=3.12"` |
| <a id="s-46dca634a2"></a>`role` | `"reusable_library"` |
| <a id="s-21998422fc"></a>`source` | `"reference/stove0/packages/api-client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-api-client](../../../evidence/relationships/nodes.md#rn-0b595a9ebc)

## Governing policies

- <a id="pa-7b46483989"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-32d9202e64"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:stove0-api-client](../../../evidence/sources/authorities.md#src-65fbc03822) — [reference/stove0/packages/api-client/pyproject.toml](../../../../../../reference/stove0/packages/api-client/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/stove0-api-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4387312aff5927947c9d0dbe04b7d92d92eac87e317b260a6096477962ed37e7 -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-api-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "reference/stove0/packages/api-client/pyproject.toml"
}
```

</details>
