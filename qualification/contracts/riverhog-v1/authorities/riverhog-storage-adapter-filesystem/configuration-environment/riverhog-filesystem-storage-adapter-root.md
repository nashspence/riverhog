# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-root:ccb68b76a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da273161b4"></a>
| Field | Shape |
|---|---|
| <a id="s-1e7b15fb47"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-71838a4472"></a>`default_expressions` | ["''"] |
| <a id="s-b1614ba9ed"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT" |
| <a id="s-55e127dee3"></a>`input_shape` | "environment-string" |
| <a id="s-8fab4aadb8"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT" |
| <a id="s-bc9e95e985"></a>`owner` | "riverhog-storage-adapter-filesystem" |

## Governing policies

- <a id="pa-c6e4ef95f7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT](../../../evidence/sources.md#src-fb670e5991) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/139`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3a15ab6f8185731011dfff5d620e16b42b13bfce9589287f94f9549840a98551 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
