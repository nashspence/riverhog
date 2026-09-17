# Installation root: piggity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-piggity:3384adf106 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-f813f65b94"></a>
| Concern | Contract |
|---|---|
| <a id="s-3bcc2b2de7"></a>`artifact_format` | `"wheel"` |
| <a id="s-1f4ca7f1b8"></a>`distribution` | `"piggity"` |
| <a id="s-ea3b9b97e0"></a>`lock` | `{"coordinate":"pylock.piggity.toml","format":"pylock.toml"}` |
| <a id="s-1f3df7c62b"></a>`method` | `"uv-tool"` |
| <a id="s-a9f704d28d"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [piggity](../../../evidence/relationships.md#rn-0a279524f4)

## Governing policies

- <a id="pa-8764ebb490"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b8ff2be8c2"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-3337e1a66f"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:piggity](../../../evidence/sources.md#src-c6f8868ecf) — [reference/riverhog/applications/piggity/pyproject.toml](../../../../../../reference/riverhog/applications/piggity/pyproject.toml)
- [release-installation:planner](../../../evidence/sources.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/piggity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0511bd0b6f91b3c551528542c85e41e8b9d1ccb86f5f104c83f3fe083b05a7fd -->

```json
{
  "artifact_format": "wheel",
  "distribution": "piggity",
  "lock": {
    "coordinate": "pylock.piggity.toml",
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
