# riverhog-storage-adapter-backblaze component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze-compon-5a06186e8e:7defe9e1e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-0b2071998706) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24b34f6c5e88"></a>
| Field | Shape |
|---|---|
| <a id="s-afb96324cf48"></a>`console_scripts` | additional keys=`riverhog-storage-adapter-backblaze` |
| <a id="s-d30b03dfed4f"></a>`dependencies` | ["riverhog-storage-adapter-asgi-support","riverhog-storage-adapter-s3-support"] |
| <a id="s-ec7a64ccc068"></a>`distribution` | "riverhog-storage-adapter-backblaze" |
| <a id="s-1d9cf6a4a2f1"></a>`optional_dependencies` | empty object |
| <a id="s-7922bb2f5597"></a>`path` | "reference/riverhog/storage/backblaze" |
| <a id="s-999a71297716"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-47273b3d10e3"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/37`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95a7f8988b0c86e176800a81be67ab0e489cd28e8b81454ea4c0db6c6543433c -->

```json
{
  "console_scripts": {
    "riverhog-storage-adapter-backblaze": "riverhog_storage_adapter_backblaze.app:main"
  },
  "dependencies": [
    "riverhog-storage-adapter-asgi-support",
    "riverhog-storage-adapter-s3-support"
  ],
  "distribution": "riverhog-storage-adapter-backblaze",
  "optional_dependencies": {},
  "path": "reference/riverhog/storage/backblaze",
  "role": "reference_component"
}
```
