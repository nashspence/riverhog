# Python distribution: a-stove0-media-archive-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-media-archive-lib:bba665ae54 -->

Media archive projection library for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-49f30d6618"></a>
| Concern | Contract |
|---|---|
| <a id="s-7262963dbb"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_media_archive_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_media_archive_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-605d7d5a4b"></a>`channel` | `"github-release"` |
| <a id="s-9209ee6982"></a>`description` | `"Media archive projection library for Stove0."` |
| <a id="s-7374c629d9"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-86d03da9e4"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-4d04ef5b07"></a>`publication_identity` | `{"coordinate":"a-stove0-media-archive-lib","kind":"python-distribution"}` |
| <a id="s-6a8ccb0a32"></a>`requires_python` | `">=3.12"` |
| <a id="s-50b43f24f9"></a>`role` | `"component"` |
| <a id="s-5c0f0da9c8"></a>`source` | `"some-implementations/stove0/targets/media-archive/support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-media-archive-lib](../../../evidence/relationships/nodes.md#rn-1ff54362d8)

## Governing policies

- <a id="pa-f2a9ee054a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-94350a2dab"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-media-archive-lib](../../../evidence/sources/authorities.md#src-163f475ff7) — [some-implementations/stove0/targets/media-archive/support/pyproject.toml](../../../../../../some-implementations/stove0/targets/media-archive/support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-media-archive-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c31182e9c53804043ab7a6eafd28837b8aeb70e323b6c0375d1f0c05ef042f45 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_media_archive_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_media_archive_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Media archive projection library for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-stove0-media-archive-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/targets/media-archive/support/pyproject.toml"
}
```

</details>
