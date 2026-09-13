# Python distribution: riverhog-archive-contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-archive-contracts:1af9604534 -->

Dependency-light immutable Riverhog archive recovery contracts.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-837b02f080"></a>
| Concern | Contract |
|---|---|
| <a id="s-17f3d8f95c"></a>`artifacts` | [{"coordinate": "dist/riverhog_archive_contracts-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_archive_contracts-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-378cb2b77a"></a>`channel` | github-release |
| <a id="s-2f281fd71a"></a>`description` | Dependency-light immutable Riverhog archive recovery contracts. |
| <a id="s-c6c6acde15"></a>`requires_python` | >=3.12 |
| <a id="s-1e476313d3"></a>`role` | reusable_library |
| <a id="s-19475f5c7c"></a>`source` | packages/riverhog-archive-contracts/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-archive-contracts](../../../evidence/relationships.md#rn-3f00f69253)

## Governing policies

- <a id="pa-e6ebdd0973"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-archive-contracts](../../../evidence/sources.md#src-f6ac304b52) — `packages/riverhog-archive-contracts/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-archive-contracts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1fc17777385c9153e72305afe880bf80264d44385ebee20c2e568e2239542d5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_archive_contracts-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_archive_contracts-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Dependency-light immutable Riverhog archive recovery contracts.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-archive-contracts/pyproject.toml"
}
```
