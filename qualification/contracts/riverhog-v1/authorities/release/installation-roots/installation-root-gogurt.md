# Installation root: gogurt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-gogurt:3a9d67a4f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-67174e8059"></a>
| Concern | Contract |
|---|---|
| <a id="s-5c51ec7761"></a>`artifact_format` | `"wheel"` |
| <a id="s-784e6a81ba"></a>`distribution` | `"gogurt"` |
| <a id="s-ae2f52fdf2"></a>`lock` | `{"coordinate":"pylock.gogurt.toml","format":"pylock.toml"}` |
| <a id="s-09a403cfab"></a>`method` | `"uv-tool"` |
| <a id="s-44c31dc790"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt](../../../evidence/relationships.md#rn-eeb2be4a71)

## Governing policies

- <a id="pa-56135d9136"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-146e6e7adf"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-68250a1f70"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:gogurt](../../../evidence/sources.md#src-3e7b582a54) — [reference/gogurt/application/pyproject.toml](../../../../../../reference/gogurt/application/pyproject.toml)
- [release-installation:planner](../../../evidence/sources.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/gogurt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8770c413e848114f77dbf5fb4899b429e3c93dd70b7628b0d43c88ae54d4bc88 -->

```json
{
  "artifact_format": "wheel",
  "distribution": "gogurt",
  "lock": {
    "coordinate": "pylock.gogurt.toml",
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
