# Python distribution: stove0-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-stove0-client:40f903afc0 -->

Optional nonnormative command-line client for the Stove0 reference application.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-211d88e28a"></a>
| Concern | Contract |
|---|---|
| <a id="s-d715afb344"></a>`artifacts` | `[{"coordinate":"dist/stove0_client-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/stove0_client-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-d12b239d97"></a>`channel` | `"github-release"` |
| <a id="s-d6f56f0fcd"></a>`description` | `"Optional nonnormative command-line client for the Stove0 reference application."` |
| <a id="s-785271ebbc"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-730e0fa40a"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-897981ecff"></a>`publication_identity` | `{"coordinate":"stove0-client","kind":"python-distribution"}` |
| <a id="s-61c47b3b27"></a>`requires_python` | `">=3.12"` |
| <a id="s-c564c8dc04"></a>`role` | `"reference_application"` |
| <a id="s-a99438e70e"></a>`source` | `"reference/stove0/application/client/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-client](../../../evidence/relationships.md#rn-c5aaef6318)

## Governing policies

- <a id="pa-b10e7eaad5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8756362e0e"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-client](../../../evidence/sources.md#src-2b4e27da80) — `reference/stove0/application/client/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/stove0-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3371c87c26e7698020451803ce0ae48a2c73a86dee75d815c0dfbd2ff2fe2037 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/stove0_client-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/stove0_client-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative command-line client for the Stove0 reference application.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "stove0-client",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_application",
  "source": "reference/stove0/application/client/pyproject.toml"
}
```

</details>
