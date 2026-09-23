# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-max-po-d8b9924b8a:5a6540d8a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f3dc096fc3"></a>

| Field | Value |
|---|---|
| <a id="s-89ecd9cd3b"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-26ca0734d7"></a>`default_expressions` | `["''"]` |
| <a id="s-201310b467"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"` |
| <a id="s-df24c034cb"></a>`input_shape` | `"environment-string"` |
| <a id="s-4baab6d3a2"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"` |
| <a id="s-7580b0bc08"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS"; consumers=["riverhog-storage-adapter-backblaze"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](#s-f3dc096fc3) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d5e74fb0ef"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-934b5209d0"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../../../evidence/sources/authorities.md#src-eba038dc56) — [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py::\_optional](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/123`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f628a68137c9b1a544f222e7982ef8faca1ef7e9fcbde50eb6008eb242868ade -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
