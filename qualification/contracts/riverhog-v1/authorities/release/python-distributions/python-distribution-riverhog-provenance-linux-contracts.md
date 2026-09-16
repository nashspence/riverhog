# Python distribution: riverhog-provenance-linux-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-l-c925bdc783:742e3715af -->

Optional nonnormative Linux observation-contract reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e6e8ca00ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-273ed36096"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_linux_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_linux_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-35267e56e0"></a>`channel` | github-release |
| <a id="s-05df902f1d"></a>`description` | Optional nonnormative Linux observation-contract reference for Riverhog provenance. |
| <a id="s-30af996bb5"></a>`license_baseline` | first-v1-publication |
| <a id="s-4f14c2df87"></a>`license_expression` | Apache-2.0 |
| <a id="s-977afa49f3"></a>`publication_identity` | {"coordinate": "riverhog-provenance-linux-contracts", "kind": "python-distribution"} |
| <a id="s-af67c3453f"></a>`requires_python` | >=3.12 |
| <a id="s-46a9e7a319"></a>`role` | reference_component |
| <a id="s-b3c12c60d7"></a>`source` | reference/riverhog/provenance/contracts/linux/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-linux-contracts](../../../evidence/relationships.md#rn-207aa064e3)

## Governing policies

- <a id="pa-4ee243433a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-12bd397e99"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-linux-contracts](../../../evidence/sources.md#src-0bc97c8819) — `reference/riverhog/provenance/contracts/linux/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-linux-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33ea9c722a3d73ebd0de271ffe5318e4a676268e03e0836da7370d023ee6d976 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_linux_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_linux_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Linux observation-contract reference for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance-linux-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/contracts/linux/pyproject.toml"
}
```

</details>
