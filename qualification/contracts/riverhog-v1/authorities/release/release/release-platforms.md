# Release platforms

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-platforms:d8a16a7667 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [release-contract](index.md#f-6cd3d52e18) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-de60655695"></a>
| Field | Shape |
|---|---|
| <a id="s-77953a86b6"></a>`end_user_artifacts` | ["linux-x64","macos-arm64","windows-x64"] |
| <a id="s-9376937321"></a>`runtime_images` | ["linux/amd64"] |

## Governing policies

- <a id="pa-2b790f1314"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

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
