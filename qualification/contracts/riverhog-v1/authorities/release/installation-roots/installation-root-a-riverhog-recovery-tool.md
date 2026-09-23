# Installation root: a-riverhog-recovery-tool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-a-riverhog-recovery-tool:a4fd49567a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-931cf06205"></a>
| Concern | Contract |
|---|---|
| <a id="s-0f4fdc5b8f"></a>`artifact_format` | `"wheel"` |
| <a id="s-4c5d3fc6d9"></a>`distribution` | `"a-riverhog-recovery-tool"` |
| <a id="s-11408fef4f"></a>`lock` | `{"coordinate":"pylock.a-riverhog-recovery-tool.toml","format":"pylock.toml"}` |
| <a id="s-ea48d996fe"></a>`method` | `"uv-tool"` |
| <a id="s-e0c34d5b6e"></a>`platforms` | `["linux-x64","macos-arm64","windows-x64"]` |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [a-riverhog-recovery-tool](../../../evidence/relationships/nodes.md#rn-418e26fced)

## Governing policies

- <a id="pa-673b9aaad1"></a>[compatibility/components/v1](../compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c99d10fedf"></a>[publication/platform-scope/v1](../../../policies/publication-platform-scope-v1/index.md#p-7dacd6d393)
- <a id="pa-f0566674e1"></a>[publication/role-retention/v1](../../../policies/publication-role-retention-v1/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release-distribution:a-riverhog-recovery-tool](../../../evidence/sources/authorities.md#src-77692cf696) — [some-implementations/riverhog/recovery/pyproject.toml](../../../../../../some-implementations/riverhog/recovery/pyproject.toml)
- [release-installation:planner](../../../evidence/sources/authorities.md#src-d1a927fc4b) — [scripts/release\_installation.py::INSTALLATION\_POLICY](../../../../../../scripts/release_installation.py)
- [release-publication:planner](../../../evidence/sources/authorities.md#src-03a2f48338) — [scripts/release.py::publication\_contract](../../../../../../scripts/release.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/publication/installation_roots/a-riverhog-recovery-tool`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7eccb885e64a76dd85e35cc2508d0c208103a303565c545b2df5f5db009450da -->

```json
{
  "artifact_format": "wheel",
  "distribution": "a-riverhog-recovery-tool",
  "lock": {
    "coordinate": "pylock.a-riverhog-recovery-tool.toml",
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
