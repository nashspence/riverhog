# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-segment-bytes:434b900372 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-f54bf7d772"></a>
| Field | Shape |
|---|---|
| <a id="s-5d62ce0db0"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-041d9e44fd"></a>`default_expressions` | ["''"] |
| <a id="s-259bb2ea54"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES" |
| <a id="s-c4049856eb"></a>`input_shape` | "environment-string" |
| <a id="s-9de26e5167"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES" |
| <a id="s-53b1b46308"></a>`owner` | "riverhog-storage-adapter-filesystem" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES"; consumers=["riverhog-storage-adapter-filesystem"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES](#s-f54bf7d772) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-c21a5731f7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-7de5f50454"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES](../../../evidence/sources.md#src-d0691ed2c9) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/140`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97f49d9207db9d43f527ef2ac8929bc13266cf56ab80d962a236f325ea7ae920 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
