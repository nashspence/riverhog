# Python distribution: riverhog-provenance-macos-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-m-a88dff0664:4243e22ac9 -->

Optional nonnormative macOS filesystem-observer reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-b24b6c7c9b"></a>
| Concern | Contract |
|---|---|
| <a id="s-1ff43f5ca8"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_macos_observer-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_macos_observer-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-25a85c1ae8"></a>`channel` | github-release |
| <a id="s-d4e880ddcc"></a>`description` | Optional nonnormative macOS filesystem-observer reference for Riverhog provenance. |
| <a id="s-c4345d9da0"></a>`license_baseline` | first-v1-publication |
| <a id="s-741409e9ad"></a>`license_expression` | Apache-2.0 |
| <a id="s-1dffd0b3f1"></a>`publication_identity` | {"coordinate": "riverhog-provenance-macos-observer", "kind": "python-distribution"} |
| <a id="s-f187b25e1e"></a>`requires_python` | >=3.12 |
| <a id="s-c0e851cde3"></a>`role` | reference_component |
| <a id="s-1e0b6cd451"></a>`source` | reference/riverhog/provenance/observers/macos/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-macos-observer](../../../evidence/relationships.md#rn-8a8a3a88aa)

## Governing policies

- <a id="pa-c282050616"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-e60bb95339"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-macos-observer](../../../evidence/sources.md#src-40294474bf) — `reference/riverhog/provenance/observers/macos/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-macos-observer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 824917cf0d2bc385d59bb1cc6c22bffccfad6a60e4284e7d5f84a3321633f6b3 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_macos_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_macos_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative macOS filesystem-observer reference for Riverhog provenance.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-provenance-macos-observer",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/observers/macos/pyproject.toml"
}
```
