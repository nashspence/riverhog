# riverhog-storage-adapter-filesystem component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem-compo-09606b0fa7:6c1d9bfa25 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-d45a257d1e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d534ae2d29"></a>
| Field | Shape |
|---|---|
| <a id="s-17063c0780"></a>`console_scripts` | additional keys=`riverhog-storage-adapter-filesystem`, `riverhog-storage-adapter-filesystem-materialize` |
| <a id="s-c7dcf9a55f"></a>`dependencies` | ["riverhog-storage-adapter-asgi-support","riverhog-storage-adapter-protocol","time-formats"] |
| <a id="s-f04bf16e0a"></a>`distribution` | "riverhog-storage-adapter-filesystem" |
| <a id="s-4a4d4768da"></a>`optional_dependencies` | empty object |
| <a id="s-41385a9789"></a>`path` | "reference/riverhog/storage/filesystem" |
| <a id="s-46884d0365"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-299cd802b9"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/38`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 886f6b1081eb2cba5d63269ceb08b550402f0a081d46399435e74b1282275d40 -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-filesystem": "riverhog_storage_adapter_filesystem.app:main",
    "riverhog-storage-adapter-filesystem-materialize": "riverhog_storage_adapter_filesystem.materialize_cli:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-protocol",
    "time-formats"
  ],
  "distribution": "riverhog-storage-adapter-filesystem",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/filesystem",
  "role": "reference_component"
}
```
