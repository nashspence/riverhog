# Python distribution: a-stove0-media-sampling-contract-lib

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-stove0-media-sampli-b45169a9ce:a91e87c569 -->

Media sampling contracts for Stove0.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-edf18101dc"></a>
| Concern | Contract |
|---|---|
| <a id="s-5f062e8a9e"></a>`artifacts` | `[{"coordinate":"dist/a_stove0_media_sampling_contract_lib-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_stove0_media_sampling_contract_lib-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-37ed40ba12"></a>`channel` | `"github-release"` |
| <a id="s-c78c22bfa6"></a>`description` | `"Media sampling contracts for Stove0."` |
| <a id="s-9095e7ce37"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-32b1c2e1d4"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-dad98d8a85"></a>`publication_identity` | `{"coordinate":"a-stove0-media-sampling-contract-lib","kind":"python-distribution"}` |
| <a id="s-8d395d7569"></a>`requires_python` | `">=3.12"` |
| <a id="s-50ddfcadbd"></a>`role` | `"component"` |
| <a id="s-1c3a0cc23e"></a>`source` | `"some-implementations/stove0/observers/contracts/media-sampling/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-media-sampling-contract-lib](../../../evidence/relationships/nodes.md#rn-3262abd36f)

## Governing policies

- <a id="pa-747a06f1ab"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-4c9c5c938f"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-media-sampling-contract-lib](../../../evidence/sources/authorities.md#src-2fc2bdd6ef) — [some-implementations/stove0/observers/contracts/media-sampling/pyproject.toml](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-stove0-media-sampling-contract-lib`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f9ade08eaa304ecf5c865abede2ee101236a3ffacfe8776c2f68c2d3653f461 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_stove0_media_sampling_contract_lib-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_stove0_media_sampling_contract_lib-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Media sampling contracts for Stove0.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-stove0-media-sampling-contract-lib",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/stove0/observers/contracts/media-sampling/pyproject.toml"
}
```

</details>
