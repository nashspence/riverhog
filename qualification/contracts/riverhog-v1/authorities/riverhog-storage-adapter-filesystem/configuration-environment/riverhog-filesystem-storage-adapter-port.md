# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-port:4d864bd0dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da1cc2577c"></a>
| Field | Shape |
|---|---|
| <a id="s-1a4b8e932d"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-3b3bfc86f1"></a>`default_expressions` | ["'8080'"] |
| <a id="s-68e3e7cef2"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT" |
| <a id="s-30e76985b7"></a>`input_shape` | "environment-string" |
| <a id="s-2d5445bb11"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT" |
| <a id="s-86be1d53d0"></a>`owner` | "riverhog-storage-adapter-filesystem" |

## Governing policies

- <a id="pa-d9b8d0524a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT](../../../evidence/sources.md#src-68937d0a09) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/137`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44c057ba35b79258812bf883eda6aac995c503b93a2993dfcb8de477d4ecdf4c -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
