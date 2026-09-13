# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-retry-mode:2e70857807 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-2c0c24555b"></a>
| Field | Shape |
|---|---|
| <a id="s-388219734c"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-0cf6ba5e8e"></a>`default_expressions` | ["''"] |
| <a id="s-aced32925f"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE" |
| <a id="s-9209aa02f6"></a>`input_shape` | "environment-string" |
| <a id="s-67fd9ad639"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE" |
| <a id="s-7ab9e38310"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-67a644618d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE](../../../evidence/sources.md#src-eb973fcaa3) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/128`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91f43231931e7a371da0a439f23ea024a2a1417099c023e60ce473fa78469692 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
