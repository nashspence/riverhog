# Python distribution: a-stove0-media-metadata-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-media-metada-c35b3a7ad6:106ab7d00d -->

Media metadata observation contracts for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-a089fac536"></a>
| Concern | Contract |
|---|---|
| <a id="s-43effa9de7"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_media_metadata_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_media_metadata_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-546a09c43c"></a>`channel` | `"github-release"` |
| <a id="s-276ee7d6c6"></a>`description` | `"Media metadata observation contracts for Stove0."` |
| <a id="s-b7e9d2d042"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-75fc5dc50a"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-4d65cdb842"></a>`publication_identity` | `{"coordinate":"a-stove0-media-metadata-contract-lib","kind":"python-distribution"}` |
| <a id="s-66ae5c97c2"></a>`requires_python` | `">=3.12"` |
| <a id="s-0f7b9cd74e"></a>`role` | `"component"` |
| <a id="s-008c84749a"></a>`source` | `"some-implementations/stove0/observers/contracts/media-metadata/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-media-metadata-contract-lib](../../../evidence/relationships/nodes.md#rn-aac630ef4d)

## Governing policies

- <a id="pa-1c47c203b8"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-5422049196"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-media-metadata-contract-lib](../../../evidence/sources/authorities.md#src-06521c8a9b) — [some-implementations/stove0/observers/contracts/media-metadata/pyproject.toml](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-media-metadata-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6fcd2a8670b36aa7bce574017cca4faa10cd770ab0f3c4879f0a67f9cfe3d286 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_media_metadata_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_media_metadata_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Media metadata observation contracts for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-stove0-media-metadata-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/observers/contracts/media-metadata/pyproject.toml"
}
```

</details>
