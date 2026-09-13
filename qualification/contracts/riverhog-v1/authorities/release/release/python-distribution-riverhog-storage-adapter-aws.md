# Python distribution: riverhog-storage-adapter-aws

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:python-distribution-riverhog-storage-adapter-aws:d129f4de76 -->

Optional nonnormative AWS storage reference for Riverhog.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-5755c0d086"></a>
| Concern | Contract |
|---|---|
| <a id="s-cc65c34eb4"></a>`artifacts` | [{"coordinate": "dist/riverhog_storage_adapter_aws-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/riverhog_storage_adapter_aws-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-2d9cb18522"></a>`channel` | github-release |
| <a id="s-38ce21d607"></a>`description` | Optional nonnormative AWS storage reference for Riverhog. |
| <a id="s-ba98c971ef"></a>`requires_python` | >=3.12 |
| <a id="s-6c226a6ffc"></a>`role` | reference_component |
| <a id="s-b2c1a6cf70"></a>`source` | reference/riverhog/storage/aws/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-storage-adapter-aws](../../../evidence/relationships.md#rn-c53f4ff3ef)

## Governing policies

- <a id="pa-d8400a2b90"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-storage-adapter-aws](../../../evidence/sources.md#src-ef11c798d9) — `reference/riverhog/storage/aws/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/riverhog-storage-adapter-aws`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffce3ab8476a3d50f540d2417fcab41b8779153748b3d1cc02704451d751eaf5 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/riverhog_storage_adapter_aws-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/riverhog_storage_adapter_aws-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative AWS storage reference for Riverhog.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/riverhog/storage/aws/pyproject.toml"
}
```
