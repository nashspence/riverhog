# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-port:578a276545 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4e217cc727"></a>
| Field | Shape |
|---|---|
| <a id="s-debc687946"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-aa39222fcf"></a>`default_expressions` | ["'8080'"] |
| <a id="s-27ea9cf097"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT" |
| <a id="s-f93c6ec479"></a>`input_shape` | "environment-string" |
| <a id="s-37722004b8"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT" |
| <a id="s-3c2dd0740f"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-5afd21b290"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT](../../../evidence/sources.md#src-4d7ca8fcb5) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}PORT', '8080')` |

### Machine authority

- `/external_contract/configuration_environment/124`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c73d6a007a30a1180728db854e78740e96e8fde9f9bf115e10753fb2e4bad86 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "'8080'"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
