# Python distribution: riverhog-provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-provenance:41a924ad5e -->

Portable Riverhog v1 per-file provenance journals and validation.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-cfee2f0d94"></a>
| Concern | Contract |
|---|---|
| <a id="s-53dcd05eda"></a>`artifacts` | [{"coordinate": "dist/riverhog_provenance-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_provenance-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-8c3203294a"></a>`channel` | github-release |
| <a id="s-ff0786b31c"></a>`description` | Portable Riverhog v1 per-file provenance journals and validation. |
| <a id="s-2f0919c65a"></a>`requires_python` | >=3.12 |
| <a id="s-68a74a45c0"></a>`role` | reusable_library |
| <a id="s-dd34473425"></a>`source` | packages/riverhog-provenance/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-provenance](../../../evidence/relationships.md#rn-728e08e7c4)

## Governing policies

- <a id="pa-56c077ab64"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-provenance](../../../evidence/sources.md#src-95dbd50af1) — `packages/riverhog-provenance/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-provenance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c9768bdb90f5b49a203227043b94c1dc1fb8fca198ca61b9bd414092e39ca7d -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_provenance-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_provenance-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable Riverhog v1 per-file provenance journals and validation.",
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "packages/riverhog-provenance/pyproject.toml"
}
```
