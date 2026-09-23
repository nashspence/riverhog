# Python distribution: a-gogurt-macos-listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-a-gogurt-macos-listener:948361cd5f -->

macOS launchd listener for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-4e5ab49b25"></a>
| Concern | Contract |
|---|---|
| <a id="s-8444279221"></a>`artifacts` | `[{"coordinate":"dist/a_gogurt_macos_listener-{version}-py3-none-any.whl","format":"wheel"},{"coordinate":"dist/a_gogurt_macos_listener-{version}.tar.gz","format":"sdist"}]` |
| <a id="s-c07754df7c"></a>`channel` | `"github-release"` |
| <a id="s-802fdaff32"></a>`description` | `"macOS launchd listener for Gogurt."` |
| <a id="s-8fb867d535"></a>`license_baseline` | `"first-v1-publication"` |
| <a id="s-ffc89bd368"></a>`license_expression` | `"Apache-2.0"` |
| <a id="s-763015bf9a"></a>`publication_identity` | `{"coordinate":"a-gogurt-macos-listener","kind":"python-distribution"}` |
| <a id="s-b6b2c9ca27"></a>`requires_python` | `">=3.12"` |
| <a id="s-f8bfd8bc44"></a>`role` | `"component"` |
| <a id="s-ff79a1afc3"></a>`source` | `"some-implementations/gogurt/listener-host/macos/pyproject.toml"` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-gogurt-macos-listener](../../../evidence/relationships/nodes.md#rn-935d3d4b6c)

## Governing policies

- <a id="pa-c4e2f90b33"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-69becb75f7"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-gogurt-macos-listener](../../../evidence/sources/authorities.md#src-93f02a2ecb) — [some-implementations/gogurt/listener-host/macos/pyproject.toml](../../../../../../some-implementations/gogurt/listener-host/macos/pyproject.toml)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/distributions/a-gogurt-macos-listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f3a4cb90b426244914ac8e7be2ee078c8f01bd805ab53060fee61b3f123c81c -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/a_gogurt_macos_listener-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/a_gogurt_macos_listener-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "macOS launchd listener for Gogurt.",
  "license_baseline": "first-v1-publication",
  "license_expression": "Apache-2.0",
  "publication_identity": {
    "coordinate": "a-gogurt-macos-listener",
    "kind": "python-distribution"
  },
  "requires_python": ">=3.12",
  "role": "component",
  "source": "some-implementations/gogurt/listener-host/macos/pyproject.toml"
}
```

</details>
