# Python distribution: http-api-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-http-api-contracts:d48e8ff655 -->

Public typed HTTP error, health, client, and operation contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1bc8b5b6b0"></a>
| Concern | Contract |
|---|---|
| <a id="s-4bb358cc93"></a>`artifacts` | `[{"coordinate":"dist/http_api_contracts-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/http_api_contracts-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-117240a1f8"></a>`channel` | `"github-release"` |
| <a id="s-a6b9167e15"></a>`description` | `"Public typed HTTP error, health, client, and operation contracts."` |
| <a id="s-cb5a5e33a2"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-41fc35ff3d"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-96f099fffe"></a>`publication_identity` | `{"coordinate":"http-api-contracts","kind":"python-distribution"}` |
| <a id="s-910a3c8817"></a>`requires_python` | `">=3.12"` |
| <a id="s-9b58a30a1d"></a>`role` | `"reusable_library"` |
| <a id="s-dbe6905ae8"></a>`source` | `"packages/http-api-contracts/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [http-api-contracts](../../../evidence/relationships.md#rn-f59c7f4102)

## Governing policies

- <a id="pa-9a4854a31f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-427d8df7cf"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:http-api-contracts](../../../evidence/sources.md#src-1554c834c7) — `packages/http-api-contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/http-api-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d0b99f6f0fbfa07af5cb12d8f6d9e26869207975ac30f4bb4668edebd12e37f -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/http_api_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/http_api_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Public typed HTTP error, health, client, and operation contracts.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "http-api-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/http-api-contracts/pyproject.toml"
}
```

</details>
