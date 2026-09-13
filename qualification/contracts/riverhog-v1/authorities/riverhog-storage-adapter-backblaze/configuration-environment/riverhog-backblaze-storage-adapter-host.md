# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-host:fa8831029f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7109d2d380"></a>
| Field | Shape |
|---|---|
| <a id="s-cb034d2b66"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-139d07d0a8"></a>`default_expressions` | ["'127.0.0.1'"] |
| <a id="s-96289d1b72"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST" |
| <a id="s-9c6d8c3217"></a>`input_shape` | "environment-string" |
| <a id="s-29050475a4"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST" |
| <a id="s-26a08e21d8"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-92a4955521"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST](../../../evidence/sources.md#src-86e7da0f27) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/121`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06300eb53f8ed3677cedb4e69f76f37383bdc18c10b197d12ec7fc6936df86f2 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
