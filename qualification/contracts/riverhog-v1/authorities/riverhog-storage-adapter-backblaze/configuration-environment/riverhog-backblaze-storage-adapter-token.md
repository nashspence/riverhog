# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-token:aa99fc0b03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb4bf336b3"></a>

| Field | Value |
|---|---|
| <a id="s-fcbee79878"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-743f1c709e"></a>`default_expressions` | `["unset"]` |
| <a id="s-2119bdad3a"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN"` |
| <a id="s-f0341e0dbc"></a>`input_shape` | `"environment-string"` |
| <a id="s-919fecd21b"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN"` |
| <a id="s-2fb35166dd"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

## Governing policies

- <a id="pa-47524a1eab"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN](../../../evidence/sources.md#src-e5a43c6259) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.environ.pop(direct_name)` |
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(direct_name)` |

### Machine authority

- `/external_contract/configuration_environment/133`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2b2716b251afbbef8c58e0dffdbbb5c990ce29f0be230bd0a765658ce50592d -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
