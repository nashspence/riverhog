# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-host:8b46dbf7e2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6299ee5d07"></a>
| Field | Shape |
|---|---|
| <a id="s-017e3c83aa"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-5a36e49f06"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-59d3e3816e"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST" |
| <a id="s-d47d216705"></a>`input_shape` | "environment-string" |
| <a id="s-2a4fc72c2d"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST" |
| <a id="s-3aebf95c60"></a>`owner` | "riverhog-storage-adapter-filesystem" |

## Governing policies

- <a id="pa-947addfec6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST](../../../evidence/sources.md#src-7bb822dcfc) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/135`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b64404dc41fcb41989c7d237aa0e4b15bb713a79d2af2de1e95bc7f4449ca77 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
