# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-root-prefix:0c53fa8e70 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-37916ffbac"></a>
| Field | Shape |
|---|---|
| <a id="s-d11e2a81c9"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-4c55035830"></a>`default_expressions` | ["''"] |
| <a id="s-f8ea685fed"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX" |
| <a id="s-a104fdac21"></a>`input_shape` | "environment-string" |
| <a id="s-55951875b9"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX" |
| <a id="s-b688a3006f"></a>`owner` | "riverhog-storage-adapter-backblaze" |

## Governing policies

- <a id="pa-c1f21af5bb"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX](../../../evidence/sources.md#src-d359d34ec3) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/129`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 915a9a37938e603484b1069d7f3eeeb90c66ab570db8ee0e330b703adb23687e -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
