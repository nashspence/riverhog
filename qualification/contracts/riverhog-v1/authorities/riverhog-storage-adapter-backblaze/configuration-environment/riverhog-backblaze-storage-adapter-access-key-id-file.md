# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-access-0462855a00:cd8919d599 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7c4bc7f8a3"></a>

| Field | Value |
|---|---|
| <a id="s-de3bbf83d5"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-603fa45834"></a>`default_expressions` | `["unset"]` |
| <a id="s-dc7c1a2e94"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE"` |
| <a id="s-45affb291c"></a>`input_shape` | `"environment-string"` |
| <a id="s-033e682cb8"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE"` |
| <a id="s-609af60b93"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

## Governing policies

- <a id="pa-6800e1be64"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE](../../../evidence/sources.md#src-9b1dab5eef) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/116`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff6ec958397f2c4b6331327aeb8cc94b61e8a10f1de697375c14c47c539722e6 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
