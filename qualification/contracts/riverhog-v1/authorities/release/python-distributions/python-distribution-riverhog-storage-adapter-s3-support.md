# Python distribution: riverhog-storage-adapter-s3-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adap-cd9ceaa79e:94848e9b44 -->

S3 store support shared by Riverhog storage adapters.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-a8fcbee2ec"></a>
| Concern | Contract |
|---|---|
| <a id="s-7337e031a5"></a>`artifacts` | `[{"coordinate":"dist/riverhog_storage_adapter_s3_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/riverhog_storage_adapter_s3_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-393df901d7"></a>`channel` | `"github-release"` |
| <a id="s-f1ef4ba7d6"></a>`description` | `"S3 store support shared by Riverhog storage adapters."` |
| <a id="s-4c4d48d853"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-3b45bb169b"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-e9e990ed16"></a>`publication_identity` | `{"coordinate":"riverhog-storage-adapter-s3-support","kind":"python-distribution"}` |
| <a id="s-3b38665c1d"></a>`requires_python` | `">=3.12"` |
| <a id="s-aeb8841285"></a>`role` | `"reusable_library"` |
| <a id="s-ec61750d2b"></a>`source` | `"some-implementations/riverhog/storage/s3-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-s3-support](../../../evidence/relationships/nodes.md#rn-b934a396f4)

## Governing policies

- <a id="pa-67c4a5fbea"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-96f53b3fa7"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-storage-adapter-s3-support](../../../evidence/sources/authorities.md#src-19239f8eca) — [some-implementations/riverhog/storage/s3-support/pyproject.toml](../../../../../../some-implementations/riverhog/storage/s3-support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-s3-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7f80a1e60a3c25edd2dff9bc43ef0835391cd6744a935c67ca9a13866581664 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_s3_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_s3_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "S3 store support shared by Riverhog storage adapters.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "riverhog-storage-adapter-s3-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/riverhog/storage/s3-support/pyproject.toml"
}
```

</details>
