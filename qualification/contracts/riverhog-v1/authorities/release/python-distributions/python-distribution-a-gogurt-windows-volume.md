# Python distribution: a-gogurt-windows-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-windows-volume:93a75640ab -->

Windows mounted-volume provider for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-c054c4ad59"></a>
| Concern | Contract |
|---|---|
| <a id="s-717dc547dc"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_windows_volume-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_windows_volume-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-a3f0c536b5"></a>`channel` | `"github-release"` |
| <a id="s-b9052d0af6"></a>`description` | `"Windows mounted-volume provider for Gogurt."` |
| <a id="s-87de8f37ad"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-a23342bccc"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-1f8bb3ad3d"></a>`publication_identity` | `{"coordinate":"a-gogurt-windows-volume","kind":"python-distribution"}` |
| <a id="s-9da6142fe7"></a>`requires_python` | `">=3.12"` |
| <a id="s-3c6d998701"></a>`role` | `"component"` |
| <a id="s-cfb2bc28e8"></a>`source` | `"some-implementations/gogurt/mounted-volume/windows/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-windows-volume](../../../evidence/relationships/nodes.md#rn-3d2f712dcf)

## Governing policies

- <a id="pa-1402e6195a"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0b1f74fe5c"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-windows-volume](../../../evidence/sources/authorities.md#src-c584e53dd2) — [some-implementations/gogurt/mounted-volume/windows/pyproject.toml](../../../../../../some-implementations/gogurt/mounted-volume/windows/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-windows-volume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 798f704d63d27ec40276c41f72f3a7b063e1e155a1bbcd6555f2ea26570bc96a -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_windows_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_windows_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Windows mounted-volume provider for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-windows-volume",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/mounted-volume/windows/pyproject.toml"
}
```

</details>
