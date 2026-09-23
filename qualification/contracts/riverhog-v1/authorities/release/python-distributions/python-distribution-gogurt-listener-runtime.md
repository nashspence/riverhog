# Python distribution: gogurt-listener-runtime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-listener-runtime:cf45e6f11a -->

Portable durable listener runtime and native-platform port for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-e542fe29c9"></a>
| Concern | Contract |
|---|---|
| <a id="s-e6233d3b53"></a>`artifacts` | `[{"coordinate":"dist/gogurt_listener_runtime-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/gogurt_listener_runtime-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-3150381b0f"></a>`channel` | `"github-release"` |
| <a id="s-fc32147bcb"></a>`description` | `"Portable durable listener runtime and native-platform port for Gogurt."` |
| <a id="s-c2b91c0691"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-feac3ceff4"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-2ff9d99d6f"></a>`publication_identity` | `{"coordinate":"gogurt-listener-runtime","kind":"python-distribution"}` |
| <a id="s-6cdafa600f"></a>`requires_python` | `">=3.12"` |
| <a id="s-30723da06a"></a>`role` | `"reusable_library"` |
| <a id="s-786d10abd4"></a>`source` | `"some-implementations/gogurt/packages/listener-runtime/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-listener-runtime](../../../evidence/relationships/nodes.md#rn-df1cdfd17c)

## Governing policies

- <a id="pa-ee7d8ad2d0"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0709c6dc3b"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt-listener-runtime](../../../evidence/sources/authorities.md#src-23725dce0b) — [some-implementations/gogurt/packages/listener-runtime/pyproject.toml](../../../../../../some-implementations/gogurt/packages/listener-runtime/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-listener-runtime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1876768ae2dc0ee46a10bf5d5ba486faccd13c356b239d75c51eb09886b149e6 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_listener_runtime-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_listener_runtime-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Portable durable listener runtime and native-platform port for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "gogurt-listener-runtime",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "reusable_library",
  "source": "some-implementations/gogurt/packages/listener-runtime/pyproject.toml"
}
```

</details>
