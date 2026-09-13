# Python distribution: riverhog-storage-adapter-backblaze

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-storage-adap-0f2f8971fa:ef762060d7 -->

Optional nonnormative Backblaze B2 storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-809e4c3631"></a>
| Concern | Contract |
|---|---|
| <a id="s-b5198770fa"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_backblaze-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_backblaze-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-26bf6a0634"></a>`channel` | github-release |
| <a id="s-87b1e43633"></a>`description` | Optional nonnormative Backblaze B2 storage reference for Riverhog. |
| <a id="s-6480cfdc45"></a>`requires_python` | >=3.12 |
| <a id="s-348118ae00"></a>`role` | reference_component |
| <a id="s-e85ec9e088"></a>`source` | reference/riverhog/storage/backblaze/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-backblaze](../../../evidence/relationships.md#rn-4178e6e967)

## Governing policies

- <a id="pa-a89d6e9194"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-backblaze](../../../evidence/sources.md#src-039f9af430) — `reference/riverhog/storage/backblaze/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-backblaze`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab7565cf0c7a0a715b2fae1c56f6877c3eb4c34afcba72184dafe0962d5fc669 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_backblaze-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_backblaze-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Backblaze B2 storage reference for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/backblaze/pyproject.toml"
}
```
