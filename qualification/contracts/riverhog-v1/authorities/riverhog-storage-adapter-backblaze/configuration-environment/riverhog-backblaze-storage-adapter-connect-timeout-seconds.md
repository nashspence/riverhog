# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-connec-2fa3fc2688:b8f8cc9740 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-c121b213d4) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f0ba6e17dd"></a>
| Field | Shape |
|---|---|
| <a id="s-9c7aa095b0"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-18c60035f0"></a>`default_expressions` | ["''"] |
| <a id="s-bc0136d5ee"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS" |
| <a id="s-0aac19877e"></a>`input_shape` | "environment-string" |
| <a id="s-3e59f086de"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS" |
| <a id="s-f783132f7f"></a>`owner` | "riverhog-storage-adapter-backblaze" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS"; consumers=["riverhog-storage-adapter-backblaze"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](#s-f0ba6e17dd) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-088866dbd5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-70a8dc37e4"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../../../evidence/sources.md#src-bb79933c34) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/118`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0443e06a9baa784b234bf8abbe5b617a29ea9b6af50fec45d00757c5c109bcf -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
