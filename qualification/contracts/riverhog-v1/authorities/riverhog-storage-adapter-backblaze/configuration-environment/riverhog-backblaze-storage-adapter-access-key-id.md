# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-access-key-id:33654234a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0cc068cac2"></a>
| Field | Shape |
|---|---|
| <a id="s-ba1e600e45"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-dfb31422c2"></a>`default_expressions` | ["unset"] |
| <a id="s-d5be6be860"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID" |
| <a id="s-de85ca3ff3"></a>`input_shape` | "environment-string" |
| <a id="s-d7bcf43917"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID" |
| <a id="s-c095dc5b0b"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-a09f4fe490"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID](../../../evidence/sources.md#src-4f13526f1c) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/115`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a997aa1c047472bdb94bcd7a889b3bab101b5290e9c443ab732330ecede8cc3 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
