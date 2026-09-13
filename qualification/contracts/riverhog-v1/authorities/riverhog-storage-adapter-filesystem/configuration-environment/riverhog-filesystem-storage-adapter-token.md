# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-token:c3c21cdbf2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0860e7403d"></a>
| Field | Shape |
|---|---|
| <a id="s-9be6b52731"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-0bd5eb779f"></a>`default_expressions` | ["unset"] |
| <a id="s-c18f78a25e"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN" |
| <a id="s-6d50564c65"></a>`input_shape` | "environment-string" |
| <a id="s-880697c149"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN" |
| <a id="s-7d7836cab8"></a>`owner` | "riverhog-storage-adapter-filesystem" |

## Governing policies

- <a id="pa-2951edd4da"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN](../../../evidence/sources.md#src-ef3b5a31c7) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/141`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1d52e2f619074038baa589801d67a3d7fbabd70ed9fba943b0edbd0bca8b6f6 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
