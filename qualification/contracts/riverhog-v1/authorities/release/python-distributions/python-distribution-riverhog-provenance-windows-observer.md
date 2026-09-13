# Python distribution: riverhog-provenance-windows-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-w-c7920d952a:2ce6836a04 -->

Optional nonnormative Windows filesystem-observer reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-048fe07e25"></a>
| Concern | Contract |
|---|---|
| <a id="s-f906eef556"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_windows_observer-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_windows_observer-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-c7b448ce40"></a>`channel` | github-release |
| <a id="s-e4b4f1365f"></a>`description` | Optional nonnormative Windows filesystem-observer reference for Riverhog provenance. |
| <a id="s-7be5fba2e2"></a>`requires_python` | >=3.12 |
| <a id="s-9650c90428"></a>`role` | reference_component |
| <a id="s-5de29928b5"></a>`source` | reference/riverhog/provenance/observers/windows/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-windows-observer](../../../evidence/relationships.md#rn-800f0c638e)

## Governing policies

- <a id="pa-8f6c0e2e22"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-4d6c9c6c85"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-windows-observer](../../../evidence/sources.md#src-39aea5b4b6) — `reference/riverhog/provenance/observers/windows/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-windows-observer`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9725476bef35e0dcc8294cbe6eae9e4d3cff88265c073b8f570be97fcd5140e6 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_windows_observer-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_windows_observer-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Windows filesystem-observer reference for Riverhog provenance.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/observers/windows/pyproject.toml"
}
```
