# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-secret-555e7b0e03:836a303020 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-8b5dcc5fb9"></a>
| Field | Shape |
|---|---|
| <a id="s-ec0c530d9d"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-0eff29d0db"></a>`default_expressions` | ["unset"] |
| <a id="s-e09e9354a8"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE" |
| <a id="s-ccf27e246d"></a>`input_shape` | "environment-string" |
| <a id="s-04d3acc49f"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE" |
| <a id="s-773e344c70"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-63ba1d73f4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE](../../../evidence/sources.md#src-3081b5b193) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/131`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3852931a5bc1a82159584c886ca01256501e4375fee44cd4e0115ee5f8dcf53 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
