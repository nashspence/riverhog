# Python distribution: riverhog-storage-adapter-backblaze

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adap-0f2f8971fa:5089aca43e -->

Optional nonnormative Backblaze B2 storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-809e4c3631"></a>
| Concern | Contract |
|---|---|
| <a id="s-b5198770fa"></a>`artifacts` | `[{"coordinate":"dist/riverhog_storage_adapter_backblaze-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_storage_adapter_backblaze-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-26bf6a0634"></a>`channel` | `"github-release"` |
| <a id="s-87b1e43633"></a>`description` | `"Optional nonnormative Backblaze B2 storage reference for Riverhog."` |
| <a id="s-ec014a4211"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-287b3d2a25"></a>`license_expression` | `"CAL-1.0"` |
| <a id="s-9518b60a51"></a>`publication_identity` | `{"coordinate":"riverhog-storage-adapter-backblaze","kind":"python-distribution"}` |
| <a id="s-6480cfdc45"></a>`requires_python` | `">=3.12"` |
| <a id="s-348118ae00"></a>`role` | `"reference_component"` |
| <a id="s-e85ec9e088"></a>`source` | `"reference/riverhog/storage/backblaze/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-backblaze](../../../evidence/relationships.md#rn-4178e6e967)

## Governing policies

- <a id="pa-07fee09554"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3a6049f7a1"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-storage-adapter-backblaze](../../../evidence/sources.md#src-039f9af430) — [reference/riverhog/storage/backblaze/pyproject.toml](../../../../../../reference/riverhog/storage/backblaze/pyproject.toml)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-backblaze`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 044b7da9a49ce5df07f70bc43ff268b94674017fcff3c2fce89589d6c2a267aa -->

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
  "license_baseline": "first-v1-publication",
  "license_expression": "CAL-1.0",
  "publication_identity": {
    "coordinate": "riverhog-storage-adapter-backblaze",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/backblaze/pyproject.toml"
}
```

</details>
