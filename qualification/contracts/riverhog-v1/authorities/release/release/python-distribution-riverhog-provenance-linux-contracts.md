# Python distribution: riverhog-provenance-linux-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-provenance-l-c925bdc783:e6ae8910c2 -->

Optional nonnormative Linux observation-contract reference for Riverhog provenance.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-e6e8ca00ea"></a>
| Concern | Contract |
|---|---|
| <a id="s-273ed36096"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance_linux_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance_linux_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-35267e56e0"></a>`channel` | github-release |
| <a id="s-05df902f1d"></a>`description` | Optional nonnormative Linux observation-contract reference for Riverhog provenance. |
| <a id="s-af67c3453f"></a>`requires_python` | >=3.12 |
| <a id="s-46a9e7a319"></a>`role` | reference_component |
| <a id="s-b3c12c60d7"></a>`source` | reference/riverhog/provenance/contracts/linux/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance-linux-contracts](../../../evidence/relationships.md#rn-207aa064e3)

## Governing policies

- <a id="pa-ab0926978a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c63aa75edfd51801c3c74494f740eeb4e0f897cf8e7b446cc7b751aa9a4f877 -->

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
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/provenance/contracts/linux/pyproject.toml"
}
```
