# Python distribution: a-stove0-media-archive-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-media-archiv-c32945c874:dfcdef9d34 -->

Media archive target contracts for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-2530eeab0c"></a>
| Concern | Contract |
|---|---|
| <a id="s-9c6080d7af"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_media_archive_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_media_archive_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-ea80c77f19"></a>`channel` | `"github-release"` |
| <a id="s-a866247d3d"></a>`description` | `"Media archive target contracts for Stove0."` |
| <a id="s-d8f7460d33"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-9c75d4f7a5"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-45d1b65946"></a>`publication_identity` | `{"coordinate":"a-stove0-media-archive-contract-lib","kind":"python-distribution"}` |
| <a id="s-0c0122a540"></a>`requires_python` | `">=3.12"` |
| <a id="s-daee9114ef"></a>`role` | `"reusable_library"` |
| <a id="s-5965d17b47"></a>`source` | `"some-implementations/stove0/targets/media-archive/contracts/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-media-archive-contract-lib](../../../evidence/relationships/nodes.md#rn-d67a828eab)

## Governing policies

- <a id="pa-35beb79f22"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-d505d37db0"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-media-archive-contract-lib](../../../evidence/sources/authorities.md#src-baadf854e2) — [some-implementations/stove0/targets/media-archive/contracts/pyproject.toml](../../../../../../some-implementations/stove0/targets/media-archive/contracts/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-media-archive-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba6a58100558221daef4fbd6b29a5d0ef22bd9403191027a4a0cb1c2e37baf4e -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_media_archive_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_media_archive_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Media archive target contracts for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-stove0-media-archive-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/stove0/targets/media-archive/contracts/pyproject.toml"
}
```

</details>
