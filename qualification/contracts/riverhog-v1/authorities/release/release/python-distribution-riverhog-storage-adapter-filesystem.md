# Python distribution: riverhog-storage-adapter-filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-storage-adap-d7f148f23f:e7dca0e9f8 -->

Optional nonnormative Linux filesystem storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-49bed7048f"></a>
| Concern | Contract |
|---|---|
| <a id="s-674e1c2ec6"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_filesystem-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_filesystem-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-7541e0b0bf"></a>`channel` | github-release |
| <a id="s-7857d0f0de"></a>`description` | Optional nonnormative Linux filesystem storage reference for Riverhog. |
| <a id="s-2ec83a96f4"></a>`requires_python` | >=3.12 |
| <a id="s-19268b4204"></a>`role` | reference_component |
| <a id="s-5d32e64c3c"></a>`source` | reference/riverhog/storage/filesystem/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-filesystem](../../../evidence/relationships.md#rn-ebe4206627)

## Governing policies

- <a id="pa-c06f46fc01"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-filesystem](../../../evidence/sources.md#src-58a272031a) — `reference/riverhog/storage/filesystem/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-filesystem`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6b90402833ccdf47dffdd46cda20da6f273249ce0d05267defee59165840ac6 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_filesystem-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_filesystem-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Linux filesystem storage reference for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/filesystem/pyproject.toml"
}
```
