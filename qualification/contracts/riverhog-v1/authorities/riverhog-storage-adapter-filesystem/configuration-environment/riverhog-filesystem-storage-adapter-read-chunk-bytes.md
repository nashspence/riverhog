# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-read-chunk-bytes:381b0f8723 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-04dd294881"></a>
| Field | Shape |
|---|---|
| <a id="s-fd706f529a"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-e4b45436b2"></a>`default_expressions` | ["''"] |
| <a id="s-e177560e94"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-05c3b10e59"></a>`input_shape` | "environment-string" |
| <a id="s-33cb89739d"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-4423e20b47"></a>`owner` | "riverhog-storage-adapter-filesystem" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES"; consumers=["riverhog-storage-adapter-filesystem"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES](#s-04dd294881) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-72dcbd6ca7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2a0e197365"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES](../../../evidence/sources.md#src-7b65a90cf1) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/138`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f9a82fe328ce667f72102867c0ee22ba41f0f221722da054bb92de7e1108820 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
