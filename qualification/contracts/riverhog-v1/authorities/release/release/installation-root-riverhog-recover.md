# Installation root: riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:installation-root-riverhog-recover:bacc9c03c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-287e73a4a5"></a>
| Concern | Contract |
|---|---|
| <a id="s-0e03f9d690"></a>`artifact_format` | wheel |
| <a id="s-3d429a1d22"></a>`distribution` | riverhog-recover |
| <a id="s-d0ccec6139"></a>`lock` | {"coordinate": "pylock.riverhog-recover.toml", "format": "pylock.toml"} |
| <a id="s-e6bd4b37f6"></a>`method` | uv-tool |
| <a id="s-ec5247a6c0"></a>`platforms` | ["linux-x64", "macos-arm64", "windows-x64"] |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [riverhog-recover](../../../evidence/relationships.md#rn-813d97e5a4)

## Governing policies

- <a id="pa-87d4f0c515"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:riverhog-recover](../../../evidence/sources.md#src-917183ebd1) — `reference/riverhog/recovery/pyproject.toml`
- [release-installation:planner](../../../evidence/sources.md#src-d1a927fc4b) — `scripts/release_installation.py::INSTALLATION_POLICY`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/installation_roots/riverhog-recover`

### Exact owned JSON

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
