# Python distribution: riverhog-storage-adapter-filesystem

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adap-d7f148f23f:ad84473518 -->

Optional nonnormative Linux filesystem storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-49bed7048f"></a>
| Concern | Contract |
|---|---|
| <a id="s-674e1c2ec6"></a>`artifacts` | `[{"coordinate":"dist/riverhog_storage_adapter_filesystem-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_storage_adapter_filesystem-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-7541e0b0bf"></a>`channel` | `"github-release"` |
| <a id="s-7857d0f0de"></a>`description` | `"Optional nonnormative Linux filesystem storage reference for Riverhog."` |
| <a id="s-32251f00f0"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-be0bb0a335"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-170f32b532"></a>`publication_identity` | `{"coordinate":"riverhog-storage-adapter-filesystem","kind":"python-distribution"}` |
| <a id="s-2ec83a96f4"></a>`requires_python` | `">=3.12"` |
| <a id="s-19268b4204"></a>`role` | `"reference_component"` |
| <a id="s-5d32e64c3c"></a>`source` | `"reference/riverhog/storage/filesystem/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-filesystem](../../../evidence/relationships/nodes.md#rn-ebe4206627)

## Governing policies

- <a id="pa-f7d5b40a52"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-b4ed04901d"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-storage-adapter-filesystem](../../../evidence/sources/authorities.md#src-58a272031a) — [reference/riverhog/storage/filesystem/pyproject.toml](../../../../../../reference/riverhog/storage/filesystem/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-filesystem`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c8b1f097955dcedf9f7ec7e96215e79c81b019b974a737f7b51f6b44fb3065d -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "riverhog-storage-adapter-filesystem",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/filesystem/pyproject.toml"
}
```

</details>
