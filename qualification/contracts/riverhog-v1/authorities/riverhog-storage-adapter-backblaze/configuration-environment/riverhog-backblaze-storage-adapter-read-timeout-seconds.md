# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-read-t-678e1c736c:e2964821f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-ee5a428565"></a>

| Field | Value |
|---|---|
| <a id="s-2023dd6f92"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-2e694ff7f4"></a>`default_expressions` | `["''"]` |
| <a id="s-0690b54cdc"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"` |
| <a id="s-cf9fff09ce"></a>`input_shape` | `"environment-string"` |
| <a id="s-8563584654"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"` |
| <a id="s-3016559a93"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS"; consumers=["riverhog-storage-adapter-backblaze"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](#s-ee5a428565) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-09d7e97e63"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-278d71a8e1"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../../../evidence/sources.md#src-7e10a8a388) — [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py::\_optional](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/126`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 418dfce24d914743fb889293853f63135e1817de62d980370b02d8c6184751a2 -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
