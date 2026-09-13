# Python distribution: riverhog-storage-adapter-s3-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-riverhog-storage-adap-cd9ceaa79e:94848e9b44 -->

Optional nonnormative S3 support for Riverhog storage references.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-a8fcbee2ec"></a>
| Concern | Contract |
|---|---|
| <a id="s-7337e031a5"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_s3_support-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_s3_support-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-393df901d7"></a>`channel` | github-release |
| <a id="s-f1ef4ba7d6"></a>`description` | Optional nonnormative S3 support for Riverhog storage references. |
| <a id="s-3b38665c1d"></a>`requires_python` | >=3.12 |
| <a id="s-aeb8841285"></a>`role` | reference_component |
| <a id="s-ec61750d2b"></a>`source` | reference/riverhog/storage/s3-support/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-s3-support](../../../evidence/relationships.md#rn-b934a396f4)

## Governing policies

- <a id="pa-67c4a5fbea"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-96f53b3fa7"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-s3-support](../../../evidence/sources.md#src-19239f8eca) — `reference/riverhog/storage/s3-support/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-s3-support`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 688418b00b15cd321694e18287b5a3a21a9764bc1fdc2f538991c2d9cf906b50 -->

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
  "description": "Optional nonnormative S3 support for Riverhog storage references.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/s3-support/pyproject.toml"
}
```
