# Python distribution: riverhog-provenance-windows-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-w-6293caaef2:2a663f3c2b -->

Optional nonnormative Windows observation-contract reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-ee830ecc22"></a>
| Concern | Contract |
|---|---|
| <a id="s-5eaa626d71"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_windows_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_windows_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-3ddbd0f699"></a>`channel` | github-release |
| <a id="s-13f8325b0d"></a>`description` | Optional nonnormative Windows observation-contract reference for Riverhog provenance. |
| <a id="s-5bef294fde"></a>`license_baseline` | first-v1-publication |
| <a id="s-6b1e645586"></a>`license_expression` | Apache-2.0 |
| <a id="s-0bf821b9c6"></a>`publication_identity` | {"coordinate": "riverhog-provenance-windows-contracts", "kind": "python-distribution"} |
| <a id="s-05f81446c0"></a>`requires_python` | >=3.12 |
| <a id="s-bd17ea25af"></a>`role` | reference_component |
| <a id="s-109bded2bc"></a>`source` | reference/riverhog/provenance/contracts/windows/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-windows-contracts](../../../evidence/relationships.md#rn-4061c95d07)

## Governing policies

- <a id="pa-15aa9e0510"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b2d273cc23"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-windows-contracts](../../../evidence/sources.md#src-3899e85b99) — `reference/riverhog/provenance/contracts/windows/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-windows-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 33fabad5d09f56f140a1a09041ab47c5582bd86c97d875997f1651bab0b9c003 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_windows_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_windows_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Windows observation-contract reference for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance-windows-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/contracts/windows/pyproject.toml"
}
```

</details>
