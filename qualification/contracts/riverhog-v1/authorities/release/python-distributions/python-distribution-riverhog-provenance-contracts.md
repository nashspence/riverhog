# Python distribution: riverhog-provenance-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-provenance-contracts:d6bb268200 -->

Canonical Riverhog provenance identity and reference contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-76c59a895f"></a>
| Concern | Contract |
|---|---|
| <a id="s-c7cd74174b"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-fcc5421583"></a>`channel` | github-release |
| <a id="s-66d5de22b4"></a>`description` | Canonical Riverhog provenance identity and reference contracts. |
| <a id="s-086db510ba"></a>`requires_python` | >=3.12 |
| <a id="s-a65cfab9bb"></a>`role` | reusable_library |
| <a id="s-bbcb7bbd23"></a>`source` | packages/riverhog-provenance-contracts/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-contracts](../../../evidence/relationships.md#rn-6066820038)

## Governing policies

- <a id="pa-e2a5e33a50"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3d92970d80"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance-contracts](../../../evidence/sources.md#src-d222cdb3b8) — `packages/riverhog-provenance-contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 144b678c00e206a3a849b432f8770177bb7576d4abfe31ea601f9e48f6ef5f13 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Canonical Riverhog provenance identity and reference contracts.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-provenance-contracts/pyproject.toml"
}
```
