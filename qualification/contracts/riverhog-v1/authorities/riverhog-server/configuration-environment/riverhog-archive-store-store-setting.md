# RIVERHOG_ARCHIVE_STORE_{store}_{setting}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-store-store-setting:8fd4fc93c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-868df593f6"></a>

| Field | Value |
|---|---|
| <a id="s-20593819b0"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-d00519f807"></a>`id` | `"riverhog-server:environment-pattern:RIVERHOG_ARCHIVE_STORE_{store}_{setting}"` |
| <a id="s-3016c4b244"></a>`input_shape` | `"environment-string"` |
| <a id="s-d1cd0ff44c"></a>`owner` | `"riverhog-server"` |
| <a id="s-b3030a17d6"></a>`parameters · setting` | `["ADAPTER_URL","ADAPTER_TOKEN_FILE","ADAPTER_ALLOW_INSECURE_HTTP","ADAPTER_MAX_CONNECTIONS","ADAPTER_TIMEOUT_SECONDS","MONTHLY_DOWNLOAD_ALLOWANCE_BYTES","DOWNLOAD_SAFETY_BUFFER_BYTES"]` |
| <a id="s-35edfdeb57"></a>`parameters · store · normalization` | `"uppercase-dashes-to-underscores"` |
| <a id="s-4d6814c764"></a>`parameters · store · source` | `"RIVERHOG_ARCHIVE_STORES"` |
| <a id="s-92cc250601"></a>`settings` | `["ADAPTER_URL","ADAPTER_TOKEN_FILE","ADAPTER_ALLOW_INSECURE_HTTP","ADAPTER_MAX_CONNECTIONS","ADAPTER_TIMEOUT_SECONDS","MONTHLY_DOWNLOAD_ALLOWANCE_BYTES","DOWNLOAD_SAFETY_BUFFER_BYTES"]` |
| <a id="s-7ba33b3116"></a>`source_symbol` | `"_archive_store_environment_name"` |
| <a id="s-e55cd62afd"></a>`template` | `"RIVERHOG_ARCHIVE_STORE_{store}_{setting}"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: classification=null; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_MAX_CONNECTIONS" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_TIMEOUT_SECONDS" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="DOWNLOAD_SAFETY_BUFFER_BYTES" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="MONTHLY_DOWNLOAD_ALLOWANCE_BYTES" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-050785ff35"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-561ab94198"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../../../evidence/sources/authorities.md#src-3071ba44b3) — [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `_archive_store_environment_name` |

### Machine authority

- `/external_contract/configuration_environment_patterns/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5df1ffa17cdfa9e963e0fb7360f6e32b8d2b25967a478894dfe8112048d6f4e4 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "id": "riverhog-server:environment-pattern:RIVERHOG_ARCHIVE_STORE_{store}_{setting}",
  "input_shape": "environment-string",
  "owner": "riverhog-server",
  "parameters": {
    "setting": [
      "ADAPTER_URL",
      "ADAPTER_TOKEN_FILE",
      "ADAPTER_ALLOW_INSECURE_HTTP",
      "ADAPTER_MAX_CONNECTIONS",
      "ADAPTER_TIMEOUT_SECONDS",
      "MONTHLY_DOWNLOAD_ALLOWANCE_BYTES",
      "DOWNLOAD_SAFETY_BUFFER_BYTES"
    ],
    "store": {
      "normalization": "uppercase-dashes-to-underscores",
      "source": "RIVERHOG_ARCHIVE_STORES"
    }
  },
  "settings": [
    "ADAPTER_URL",
    "ADAPTER_TOKEN_FILE",
    "ADAPTER_ALLOW_INSECURE_HTTP",
    "ADAPTER_MAX_CONNECTIONS",
    "ADAPTER_TIMEOUT_SECONDS",
    "MONTHLY_DOWNLOAD_ALLOWANCE_BYTES",
    "DOWNLOAD_SAFETY_BUFFER_BYTES"
  ],
  "source_symbol": "_archive_store_environment_name",
  "template": "RIVERHOG_ARCHIVE_STORE_{store}_{setting}"
}
```

</details>
