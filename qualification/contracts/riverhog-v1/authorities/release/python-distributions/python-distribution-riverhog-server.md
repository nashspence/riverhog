# Python distribution: riverhog-server

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-server:26af6f96a6 -->

Encrypted archive management, catalog, and retrieval.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ef5e4be0c6"></a>
| Concern | Contract |
|---|---|
| <a id="s-f7cb7def60"></a>`artifacts` | `[{"coordinate":"dist/riverhog_server-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_server-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-1c39439628"></a>`channel` | `"github-release"` |
| <a id="s-0106c03018"></a>`description` | `"Encrypted archive management, catalog, and retrieval."` |
| <a id="s-a492af8c5d"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-9042380386"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-7657fcc355"></a>`publication_identity` | `{"coordinate":"riverhog-server","kind":"python-distribution"}` |
| <a id="s-8b9f2217af"></a>`requires_python` | `">=3.12"` |
| <a id="s-91a9c07764"></a>`role` | `"deployed_implementation"` |
| <a id="s-2f3bc56f46"></a>`source` | `"riverhog/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-server](../../../evidence/relationships.md#rn-807c63322e)

## Governing policies

- <a id="pa-2b26a353f7"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-316a412873"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-server](../../../evidence/sources.md#src-7debc5c818) — `riverhog/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-server`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1e23edb70fec21e84c6e0eb0fe7ed32492f1d950d7df5d0da69acc0c6ef3bb3 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_server-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_server-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Encrypted archive management, catalog, and retrieval.",
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "riverhog-server",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "deployed_implementation",
  "source": "riverhog/pyproject.toml"
}
```

</details>
