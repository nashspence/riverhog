# Installation root: riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-riverhog-recover:de67b58acb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-287e73a4a5"></a>
| Concern | Contract |
|---|---|
| <a id="s-0e03f9d690"></a>`artifact_format` | `"wheel"` |
| <a id="s-3d429a1d22"></a>`distribution` | `"riverhog-recover"` |
| <a id="s-d0ccec6139"></a>`lock` | `{"coordinate":"pylock.riverhog-recover.toml","format":"pylock.toml"}` |
| <a id="s-e6bd4b37f6"></a>`method` | `"uv-tool"` |
| <a id="s-ec5247a6c0"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-recover](../../../evidence/relationships.md#rn-813d97e5a4)

## Governing policies

- <a id="pa-3f7b8ca5a1"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-45a4ddc887"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-b30b86dcdc"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:riverhog-recover](../../../evidence/sources.md#src-917183ebd1) — [reference/riverhog/recovery/pyproject.toml](../../../../../../reference/riverhog/recovery/pyproject.toml)
- [release-installation:planner](../../../evidence/sources.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/riverhog-recover`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e2686749f5b50c31f4b503b8a0a849521ca578b553fbb3cb3d90e5fabd202b1 -->

```json
{
  "artifact_format": "wheel",
  "distribution": "riverhog-recover",
  "lock": {
    "coordinate": "pylock.riverhog-recover.toml",
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
