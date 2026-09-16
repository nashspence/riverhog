# Installation root: stove0-client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: installation-roots:release:installation-root-stove0-client:9bc5a63a73 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Installation Roots](index.md) |

## External contract

<a id="s-0bba6207bf"></a>
| Concern | Contract |
|---|---|
| <a id="s-d482a4717c"></a>`artifact_format` | wheel |
| <a id="s-3f04ad09d6"></a>`distribution` | stove0-client |
| <a id="s-f9519ac1c8"></a>`lock` | {"coordinate": "pylock.stove0-client.toml", "format": "pylock.toml"} |
| <a id="s-879a5e966e"></a>`method` | uv-tool |
| <a id="s-498cf60982"></a>`platforms` | ["linux-x64", "macos-arm64", "windows-x64"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [stove0-client](../../../evidence/relationships.md#rn-c5aaef6318)

## Governing policies

- <a id="pa-39e80ae17a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-40cedbbcfe"></a>[publication/platform-scope/v1](../../../policies/index.md#p-7dacd6d393)
- <a id="pa-ac2da71cf4"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:stove0-client](../../../evidence/sources.md#src-2b4e27da80) — `reference/stove0/application/client/pyproject.toml`
- [release-installation:planner](../../../evidence/sources.md#src-d1a927fc4b) — `scripts/release_installation.py::INSTALLATION_POLICY`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/installation_roots/stove0-client`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 068ce19a8f47cc4f9df74787bb048094f5ccba2954850dd3a9ee5ef6b8df0cde -->

```json
{
  "artifact_format": "wheel",
  "distribution": "stove0-client",
  "lock": {
    "coordinate": "pylock.stove0-client.toml",
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
