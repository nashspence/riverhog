# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-tcp-keepalive:0476919c29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd353547c1"></a>

| Field | Value |
|---|---|
| <a id="s-99a088c360"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-82eb3d9be1"></a>`default_expressions` | `["''"]` |
| <a id="s-ba76881dc9"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE"` |
| <a id="s-217d52381b"></a>`input_shape` | `"environment-string"` |
| <a id="s-4c310f1da8"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE"` |
| <a id="s-da6797fecf"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

## Governing policies

- <a id="pa-9bc25a03a7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE](../../../evidence/sources.md#src-ca135410c6) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/132`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4848ca092b890aac6dffd3d7b57fdc8ec6cb50e7bc712a6592cce8001c0d7cfe -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
