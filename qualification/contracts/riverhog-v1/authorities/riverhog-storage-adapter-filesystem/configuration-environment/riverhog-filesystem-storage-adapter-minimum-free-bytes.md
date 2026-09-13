# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-minim-cf4e807510:49f2d4e37f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d13e9ae004"></a>
| Field | Shape |
|---|---|
| <a id="s-f0e3fee02f"></a>`consumers` | ["riverhog-storage-adapter-filesystem"] |
| <a id="s-e3d6152482"></a>`default_expressions` | ["''"] |
| <a id="s-e0d8eea5f2"></a>`id` | "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES" |
| <a id="s-4c3d51fadb"></a>`input_shape` | "environment-string" |
| <a id="s-2c255b04b2"></a>`name` | "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES" |
| <a id="s-d26a81b88c"></a>`owner` | "riverhog-storage-adapter-filesystem" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES"; consumers=["riverhog-storage-adapter-filesystem"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](#s-d13e9ae004) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-3eac13baa5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-27b65ec96e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](../../../evidence/sources.md#src-3a1b9a87f7) — `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | `reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py` | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/136`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cca86b5491b538b768d257375229203b715bde979da3bccd5ad835135a167fd -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-filesystem"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES",
  "owner": "riverhog-storage-adapter-filesystem"
}
```
