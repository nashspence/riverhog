# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-bucket:a478f8867f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-03d76831f6"></a>
| Field | Shape |
|---|---|
| <a id="s-9d176682fc"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-6af6fd9c96"></a>`default_expressions` | ["''"] |
| <a id="s-70fa9907ac"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET" |
| <a id="s-79dd557f69"></a>`input_shape` | "environment-string" |
| <a id="s-709f3f0d7c"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET" |
| <a id="s-3cae61146e"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-016c675d56"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET](../../../evidence/sources.md#src-462fbc2e65) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/117`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e9da63e276210fe3f7ad73ea17d18fadb55bf2d006a497af42b36a706243ba0 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
