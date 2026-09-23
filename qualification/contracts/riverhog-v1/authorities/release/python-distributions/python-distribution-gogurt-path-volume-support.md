# Python distribution: gogurt-path-volume-support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-path-volume-support:1157076616 -->

Path-mounted volume support shared by Gogurt providers.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-970e03193f"></a>
| Concern | Contract |
|---|---|
| <a id="s-f03078e187"></a>`artifacts` | `[{"coordinate":"dist/gogurt_path_volume_support-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt_path_volume_support-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-011564d47e"></a>`channel` | `"github-release"` |
| <a id="s-6fba35ead7"></a>`description` | `"Path-mounted volume support shared by Gogurt providers."` |
| <a id="s-f51e8e45bd"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-36bd7906dd"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-0cb1bbb038"></a>`publication_identity` | `{"coordinate":"gogurt-path-volume-support","kind":"python-distribution"}` |
| <a id="s-1bfa711b80"></a>`requires_python` | `">=3.12"` |
| <a id="s-0e52d9bd37"></a>`role` | `"reusable_library"` |
| <a id="s-3701eb1b30"></a>`source` | `"some-implementations/gogurt/mounted-volume/path-support/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-path-volume-support](../../../evidence/relationships/nodes.md#rn-1b4c9bf55f)

## Governing policies

- <a id="pa-0fe667658f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-44990df4c1"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt-path-volume-support](../../../evidence/sources/authorities.md#src-fc01fccb44) — [some-implementations/gogurt/mounted-volume/path-support/pyproject.toml](../../../../../../some-implementations/gogurt/mounted-volume/path-support/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-path-volume-support`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6650ac1dcfd629c800d3283a548ad450fa7082a484708d2a7e86f0a78ecc3aef -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_path_volume_support-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_path_volume_support-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Path-mounted volume support shared by Gogurt providers.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-path-volume-support",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/gogurt/mounted-volume/path-support/pyproject.toml"
}
```

</details>
