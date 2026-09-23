# Installation root: a-stove0-cli

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-a-stove0-cli:1b3e032daf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-98b148eea8"></a>
| Concern | Contract |
|---|---|
| <a id="s-6687440687"></a>`artifact_format` | `"wheel"` |
| <a id="s-b2a715e382"></a>`distribution` | `"a-stove0-cli"` |
| <a id="s-b0cea02efa"></a>`lock` | `{"coordinate":"pylock.a-stove0-cli.toml","format":"pylock.toml"}` |
| <a id="s-5d68d9615a"></a>`method` | `"uv-tool"` |
| <a id="s-651210cd02"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-stove0-cli](../../../evidence/relationships/nodes.md#rn-acec7cb547)

## Governing policies

- <a id="pa-f5bb29c6ea"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-1ddbeb90a2"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-35a82eab7f"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-stove0-cli](../../../evidence/sources/authorities.md#src-1a670e9d1d) — [some-implementations/stove0/application/client/pyproject.toml](../../../../../../some-implementations/stove0/application/client/pyproject.toml)
- [release-installation:planner](../../../evidence/sources/authorities.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/a-stove0-cli`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec1037b8d231381898f4c90c77e80450d0870eaf86c7aa757f39081df740bd96 -->

```json
{
  "artifact_format": "wheel",
  "distribution": "a-stove0-cli",
  "lock": {
    "coordinate": "pylock.a-stove0-cli.toml",
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
