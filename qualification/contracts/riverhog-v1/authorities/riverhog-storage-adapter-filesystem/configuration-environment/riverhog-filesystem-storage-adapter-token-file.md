# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-token-file:8f604e4652 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8317358e2c"></a>

| Field | Value |
|---|---|
| <a id="s-199aa9f73a"></a>`consumers` | `["riverhog-storage-adapter-filesystem"]` |
| <a id="s-d1c9c6fea0"></a>`default_expressions` | `["unset"]` |
| <a id="s-bda4de6525"></a>`id` | `"riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-e4613e0fe4"></a>`input_shape` | `"environment-string"` |
| <a id="s-62adb1af86"></a>`name` | `"RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-9a4963c48d"></a>`owner` | `"riverhog-storage-adapter-filesystem"` |

## Governing policies

- <a id="pa-ab6043cebf"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE](../../../evidence/sources.md#src-db67f94747) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/app.py::\_secret](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/app.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/142`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18b19c384fe1a7598d3620a534d6623f6790f1396b25f3ce0aed36a56848630d -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE",
  "owner": "riverhog-storage-adapter-filesystem"
}
```

</details>
