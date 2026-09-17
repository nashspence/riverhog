# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-token-file:b287b5ef65 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-830a33f58f"></a>

| Field | Value |
|---|---|
| <a id="s-33dee50a09"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-d60d0efc38"></a>`default_expressions` | `["unset"]` |
| <a id="s-1134d2a2b0"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-cf5d831d6b"></a>`input_shape` | `"environment-string"` |
| <a id="s-d6448f56b3"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE"` |
| <a id="s-faa4c8819d"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

## Governing policies

- <a id="pa-c7a5aca7ca"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE](../../../evidence/sources/authorities.md#src-487ca76c79) — [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py::\_secret](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py) | `os.getenv(file_name)` |

### Machine authority

- `/external_contract/configuration_environment/134`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 346f7c86639b5036a4c1c0c29fd9ddff2df97bfeb1f8c69b6fa757083ef1084f -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
