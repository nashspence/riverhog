# Python distribution: gogurt-macos-mounted-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-macos-mounted-volume:93ae4de12f -->

Optional nonnormative macOS mounted-volume reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-9a95acaa83"></a>
| Concern | Contract |
|---|---|
| <a id="s-bd8eb6ba21"></a>`artifacts` | `[{"coordinate":"dist/gogurt_macos_mounted_volume-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt_macos_mounted_volume-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-4127876bfb"></a>`channel` | `"github-release"` |
| <a id="s-daac4b9373"></a>`description` | `"Optional nonnormative macOS mounted-volume reference for Gogurt."` |
| <a id="s-fb021f2e0b"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-bcbce45519"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-11187df849"></a>`publication_identity` | `{"coordinate":"gogurt-macos-mounted-volume","kind":"python-distribution"}` |
| <a id="s-6101c9b7b3"></a>`requires_python` | `">=3.12"` |
| <a id="s-edfbc33440"></a>`role` | `"reference_component"` |
| <a id="s-bceb97ae45"></a>`source` | `"reference/gogurt/mounted-volume/macos/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-macos-mounted-volume](../../../evidence/relationships/nodes.md#rn-36cc5f6cd2)

## Governing policies

- <a id="pa-652ecdd588"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-217ce718f2"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt-macos-mounted-volume](../../../evidence/sources/authorities.md#src-ea30a89c8b) — [reference/gogurt/mounted-volume/macos/pyproject.toml](../../../../../../reference/gogurt/mounted-volume/macos/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-macos-mounted-volume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7ab8f4c141be6fbbea2d6948faff60b94067dad07154a7f5b3f1d4a3bfb0d22 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_macos_mounted_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_macos_mounted_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative macOS mounted-volume reference for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-macos-mounted-volume",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/mounted-volume/macos/pyproject.toml"
}
```

</details>
