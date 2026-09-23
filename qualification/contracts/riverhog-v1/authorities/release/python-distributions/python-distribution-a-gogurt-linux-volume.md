# Python distribution: a-gogurt-linux-volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-linux-volume:4cba42ae16 -->

Linux mounted-volume provider for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-115c372c63"></a>
| Concern | Contract |
|---|---|
| <a id="s-e3df9b0652"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_linux_volume-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_linux_volume-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-62765cef94"></a>`channel` | `"github-release"` |
| <a id="s-b48afdaff6"></a>`description` | `"Linux mounted-volume provider for Gogurt."` |
| <a id="s-325ee0ce3e"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-875110c314"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-094d7602fd"></a>`publication_identity` | `{"coordinate":"a-gogurt-linux-volume","kind":"python-distribution"}` |
| <a id="s-076cb86967"></a>`requires_python` | `">=3.12"` |
| <a id="s-ff900ddf4d"></a>`role` | `"component"` |
| <a id="s-d031022763"></a>`source` | `"some-implementations/gogurt/mounted-volume/linux/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-linux-volume](../../../evidence/relationships/nodes.md#rn-6669aa00a8)

## Governing policies

- <a id="pa-6f062ed0d9"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-787416300b"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-linux-volume](../../../evidence/sources/authorities.md#src-e7942b4ba7) — [some-implementations/gogurt/mounted-volume/linux/pyproject.toml](../../../../../../some-implementations/gogurt/mounted-volume/linux/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-linux-volume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 110d3f7ce6b06b321a05def5e953fd6c197b40e5ec6d1e8d5c3f11d22aba7955 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_linux_volume-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_linux_volume-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Linux mounted-volume provider for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-linux-volume",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/mounted-volume/linux/pyproject.toml"
}
```

</details>
