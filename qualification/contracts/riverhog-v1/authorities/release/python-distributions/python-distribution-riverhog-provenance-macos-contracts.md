# Python distribution: riverhog-provenance-macos-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-m-0caf1a5786:5b037ed6bc -->

Optional nonnormative macOS observation-contract reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e9e65a70fd"></a>
| Concern | Contract |
|---|---|
| <a id="s-a17e8139b2"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_macos_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_macos_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-76df453bbb"></a>`channel` | github-release |
| <a id="s-32ee5ce9e0"></a>`description` | Optional nonnormative macOS observation-contract reference for Riverhog provenance. |
| <a id="s-f425b5ddfa"></a>`license_baseline` | first-v1-publication |
| <a id="s-654864b538"></a>`license_expression` | Apache-2.0 |
| <a id="s-0e7f9d1092"></a>`publication_identity` | {"coordinate": "riverhog-provenance-macos-contracts", "kind": "python-distribution"} |
| <a id="s-5b5806894c"></a>`requires_python` | >=3.12 |
| <a id="s-117a30aed3"></a>`role` | reference_component |
| <a id="s-478e1401b2"></a>`source` | reference/riverhog/provenance/contracts/macos/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-macos-contracts](../../../evidence/relationships.md#rn-5674e1f4d9)

## Governing policies

- <a id="pa-f51a7968ae"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0fca2edcc1"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-macos-contracts](../../../evidence/sources.md#src-7b7622c15a) — `reference/riverhog/provenance/contracts/macos/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-macos-contracts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15766c9bb9447340ccbf950ad06a8196125bf2f39e336d500f42c8b1c9266587 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_macos_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_macos_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative macOS observation-contract reference for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance-macos-contracts",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/contracts/macos/pyproject.toml"
}
```

</details>
