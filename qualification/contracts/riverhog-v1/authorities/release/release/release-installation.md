# Release installation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-installation:63e13a1ed0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [release-contract](index.md#f-6cd3d52e18) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-591d9277f3"></a>
| Field | Shape |
|---|---|
| <a id="s-99101f7c3b"></a>`listener` | additional keys=`autorun`, `provider_identity`, `provider_selection`, `resume`, `root`, `scope` |
| <a id="s-e00739269b"></a>`lock_format` | "pylock.toml" |
| <a id="s-e5f4f53934"></a>`managed_python` | true |
| <a id="s-dc3118aa9c"></a>`method` | "uv-tool" |
| <a id="s-465ad70c5b"></a>`roots` | ["gogurt","piggity","riverhog-recover","stove0-client"] |
| <a id="s-edcf49c093"></a>`simple_index_path` | "artifacts/v{version}/simple/" |
| <a id="s-215525b84b"></a>`wheel_only` | true |

## Governing policies

- <a id="pa-fca9702fca"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/installation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 630504f7554a37df84f3b9370d2936a2f64a98eaef8bdc31f98b5e3eac0face8 -->

```json
{
  "listener": {
    "autorun": "explicit-required",
    "provider_identity": "persisted-exactly",
    "provider_selection": "explicit",
    "resume": "next-login",
    "root": "gogurt",
    "scope": "current-user"
  },
  "lock_format": "pylock.toml",
  "managed_python": true,
  "method": "uv-tool",
  "roots": [
    "gogurt",
    "piggity",
    "riverhog-recover",
    "stove0-client"
  ],
  "simple_index_path": "artifacts/v{version}/simple/",
  "wheel_only": true
}
```
