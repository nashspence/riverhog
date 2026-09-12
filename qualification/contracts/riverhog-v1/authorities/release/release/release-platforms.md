# Release platforms

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-platforms:d8a16a7667 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [release-contract](index.md#f-6cd3d52e18f6) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-de60655695b6"></a>
| Field | Shape |
|---|---|
| <a id="s-77953a86b61d"></a>`end_user_artifacts` | ["linux-x64","macos-arm64","windows-x64"] |
| <a id="s-93769373213c"></a>`runtime_images` | ["linux/amd64"] |

## Governing policies

- <a id="pa-2b790f131426"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/external_contract/release/platforms`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e16534e00d354ad33a8fd8bbc75ac109eeac3a9f7c45fb8e20277d7fdb3f12ca -->

```json
{
  "end_user_artifacts": [
    "linux-x64",
    "macos-arm64",
    "windows-x64"
  ],
  "runtime_images": [
    "linux/amd64"
  ]
}
```
