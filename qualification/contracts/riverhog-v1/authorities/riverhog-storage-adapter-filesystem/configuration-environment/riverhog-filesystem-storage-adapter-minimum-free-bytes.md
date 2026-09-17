# RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-filesystem:riverhog-filesystem-storage-adapter-minim-cf4e807510:49f2d4e37f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-filesystem](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d13e9ae004"></a>

| Field | Value |
|---|---|
| <a id="s-f0e3fee02f"></a>`consumers` | `["riverhog-storage-adapter-filesystem"]` |
| <a id="s-e3d6152482"></a>`default_expressions` | `["''"]` |
| <a id="s-e0d8eea5f2"></a>`id` | `"riverhog-storage-adapter-filesystem:environment:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES"` |
| <a id="s-4c3d51fadb"></a>`input_shape` | `"environment-string"` |
| <a id="s-2c255b04b2"></a>`name` | `"RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES"` |
| <a id="s-d26a81b88c"></a>`owner` | `"riverhog-storage-adapter-filesystem"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES"; consumers=["riverhog-storage-adapter-filesystem"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](#s-d13e9ae004) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-3eac13baa5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-27b65ec96e"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](../../../evidence/sources/authorities.md#src-3a1b9a87f7) — [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/app.py::\_optional](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-filesystem` | [reference/riverhog/storage/filesystem/src/riverhog\_storage\_adapter\_filesystem/app.py](../../../../../../reference/riverhog/storage/filesystem/src/riverhog_storage_adapter_filesystem/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/136`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
