# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-endpoint-url:6ac7770d40 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7a1e93aef1"></a>
| Field | Shape |
|---|---|
| <a id="s-31f233c17f"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-4e6795016d"></a>`default_expressions` | ["''"] |
| <a id="s-8b573f8c6b"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL" |
| <a id="s-8fde01c655"></a>`input_shape` | "environment-string" |
| <a id="s-be64f6ddb2"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL" |
| <a id="s-8669d543f0"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-a10450418a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL](../../../evidence/sources.md#src-4667488734) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/119`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7683d930f0718db5dbc9e632b0e5371346fcf1c4d0131ad2cf0161e87aa8bc4e -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
