# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-region:255abb0f4a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-369e6c5b50"></a>
| Field | Shape |
|---|---|
| <a id="s-8820447a9b"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-9840fd3088"></a>`default_expressions` | ["''"] |
| <a id="s-02bae8fe60"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION" |
| <a id="s-f6dbd4d759"></a>`input_shape` | "environment-string" |
| <a id="s-db6f7ec6d4"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION" |
| <a id="s-df0ee5554e"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-30d4eb18c1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION](../../../evidence/sources.md#src-7866c27522) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(variable, '')` |

### Machine authority

- `/external_contract/configuration_environment/127`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c767261fc496e0a543198018292e57994a968e269b69cd230cdd0976b4917d73 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
