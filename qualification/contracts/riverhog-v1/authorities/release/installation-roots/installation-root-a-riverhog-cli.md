# Installation root: a-riverhog-cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-a-riverhog-cli:871764f80d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-e2d04d5d63"></a>
| Concern | Contract |
|---|---|
| <a id="s-7f45043997"></a>`artifact_format` | `"wheel"` |
| <a id="s-8b67b40032"></a>`distribution` | `"a-riverhog-cli"` |
| <a id="s-69adb7677d"></a>`lock` | `{"coordinate":"pylock.a-riverhog-cli.toml","format":"pylock.toml"}` |
| <a id="s-c323636e0e"></a>`method` | `"uv-tool"` |
| <a id="s-3270dd6855"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-cli](../../../evidence/relationships/nodes.md#rn-96e1191c92)

## Governing policies

- <a id="pa-cca3d7096f"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-ea4e1b5788"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-1805f99c7b"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d244665544) — [some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/pyproject.toml)
- [release-installation:planner](../../../evidence/sources/authorities.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/a-riverhog-cli`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 536153506c873e9028c8e8ff73893d3fa8c2241d60c189fe5947c99f53f4504f -->

```json
{
  "artifact_format": "wheel",
  "distribution": "a-riverhog-cli",
  "lock": {
    "coordinate": "pylock.a-riverhog-cli.toml",
    "format": "pylock.toml"
  },
  "method": "uv-tool",
  "platforms": [
    "linux-x64",
    "macos-arm64",
    "windows-x64"
  ]
}
```

</details>
