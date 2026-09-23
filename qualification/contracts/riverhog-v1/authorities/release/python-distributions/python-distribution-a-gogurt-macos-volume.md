# Python distribution: a-gogurt-macos-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-macos-volume:7186435e55 -->

macOS mounted-volume provider for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-1d2f433035"></a>
| Concern | Contract |
|---|---|
| <a id="s-03a2e237fc"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_macos_volume-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_macos_volume-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-ad52ab6457"></a>`channel` | `"github-release"` |
| <a id="s-823d97d5e2"></a>`description` | `"macOS mounted-volume provider for Gogurt."` |
| <a id="s-b23a515f48"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-d49de7f7d6"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2a362a175d"></a>`publication_identity` | `{"coordinate":"a-gogurt-macos-volume","kind":"python-distribution"}` |
| <a id="s-d1cb48ff50"></a>`requires_python` | `">=3.12"` |
| <a id="s-82797c24dd"></a>`role` | `"component"` |
| <a id="s-403a620ff8"></a>`source` | `"some-implementations/gogurt/mounted-volume/macos/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-macos-volume](../../../evidence/relationships/nodes.md#rn-8d835e7788)

## Governing policies

- <a id="pa-9aca597f3c"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-247304fae9"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-macos-volume](../../../evidence/sources/authorities.md#src-bf3f38b613) — [some-implementations/gogurt/mounted-volume/macos/pyproject.toml](../../../../../../some-implementations/gogurt/mounted-volume/macos/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-macos-volume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cff1a7a290490c06accd6668dbd76c6a48b2410ee4edc1671ab563c01f64c31c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_macos_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_macos_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "macOS mounted-volume provider for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-macos-volume",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/mounted-volume/macos/pyproject.toml"
}
```

</details>
