# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-force-path-style:c8a86d905e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-965179fc46"></a>

| Field | Value |
|---|---|
| <a id="s-4433b3c434"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-83ba75a7bc"></a>`default_expressions` | `["''"]` |
| <a id="s-efd9f29cdd"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE"` |
| <a id="s-7284882569"></a>`input_shape` | `"environment-string"` |
| <a id="s-4dd5bfca84"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE"` |
| <a id="s-7d68affc90"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

## Governing policies

- <a id="pa-db3ab8c5ef"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE](../../../evidence/sources.md#src-5ebbe5a454) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/120`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cb45c7e8364d36f2bdf9ac2e26e00d5ec244b7a9508750c319f8a1a0071303d -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
