# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-read-chunk-bytes:6d63680710 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-c121b213d4) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-25dea1f42a"></a>
| Field | Shape |
|---|---|
| <a id="s-648007d91b"></a>`consumers` | ["riverhog-storage-adapter-backblaze"] |
| <a id="s-96a6dd7dd7"></a>`default_expressions` | ["''"] |
| <a id="s-8ff65ad249"></a>`id` | "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-61c4702084"></a>`input_shape` | "environment-string" |
| <a id="s-79164d1f48"></a>`name` | "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES" |
| <a id="s-5908522b13"></a>`owner` | "riverhog-storage-adapter-backblaze" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES"; consumers=["riverhog-storage-adapter-backblaze"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES](#s-25dea1f42a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-8f5e222171"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-964a19411f"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES](../../../evidence/sources.md#src-8a38b0e993) — `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/125`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4eec74ef04fd1f4725f8708ccbe47faf2147fb9fc4a52d8df14b6c3e5e1fedbd -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES",
  "owner": "riverhog-storage-adapter-backblaze"
}
```
