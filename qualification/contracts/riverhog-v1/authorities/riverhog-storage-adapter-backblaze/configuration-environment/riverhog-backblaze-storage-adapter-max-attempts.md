# RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-storage-adapter-backblaze:riverhog-backblaze-storage-adapter-max-attempts:8f77cb5419 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-backblaze](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1c81252996"></a>

| Field | Value |
|---|---|
| <a id="s-841c1e0c0e"></a>`consumers` | `["riverhog-storage-adapter-backblaze"]` |
| <a id="s-b96c482685"></a>`default_expressions` | `["''"]` |
| <a id="s-ae1c3e3714"></a>`id` | `"riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS"` |
| <a id="s-74bf540ff3"></a>`input_shape` | `"environment-string"` |
| <a id="s-cfc1e16201"></a>`name` | `"RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS"` |
| <a id="s-f76569877b"></a>`owner` | `"riverhog-storage-adapter-backblaze"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS"; consumers=["riverhog-storage-adapter-backblaze"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS](#s-1c81252996) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-339f2dfb14"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-999469770a"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS](../../../evidence/sources/authorities.md#src-2072a40ae0) — [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py::\_optional](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-storage-adapter-backblaze` | [reference/riverhog/storage/backblaze/src/riverhog\_storage\_adapter\_backblaze/app.py](../../../../../../reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/122`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 853da67b6d6ca508f2ecee9c0a9213527c636525031309a6d3b4eeee1e7beeeb -->

```json
{
  "consumers": [
    "riverhog-storage-adapter-backblaze"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-storage-adapter-backblaze:environment:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS",
  "owner": "riverhog-storage-adapter-backblaze"
}
```

</details>
