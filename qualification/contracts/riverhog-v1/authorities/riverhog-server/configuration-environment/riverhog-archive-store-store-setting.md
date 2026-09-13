# RIVERHOG_ARCHIVE_STORE_{store}_{setting}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-store-store-setting:8fd4fc93c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-868df593f6"></a>
| Field | Shape |
|---|---|
| <a id="s-20593819b0"></a>`consumers` | ["riverhog-server"] |
| <a id="s-d00519f807"></a>`id` | "riverhog-server:environment-pattern:RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |
| <a id="s-3016c4b244"></a>`input_shape` | "environment-string" |
| <a id="s-d1cd0ff44c"></a>`owner` | "riverhog-server" |
| <a id="s-84c52d0e1b"></a>`parameters` | additional keys=`setting`, `store` |
| <a id="s-92cc250601"></a>`settings` | ["ADAPTER_URL","ADAPTER_TOKEN_FILE","ADAPTER_ALLOW_INSECURE_HTTP","ADAPTER_MAX_CONNECTIONS","ADAPTER_TIMEOUT_SECONDS","MONTHLY_DOWNLOAD_ALLOWANCE_BYTES","DOWNLOAD_SAFETY_BUFFER_BYTES"] |
| <a id="s-7ba33b3116"></a>`source_symbol` | "_archive_store_environment_name" |
| <a id="s-e55cd62afd"></a>`template` | "RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: classification=null; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_MAX_CONNECTIONS" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_TIMEOUT_SECONDS" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="DOWNLOAD_SAFETY_BUFFER_BYTES" |
| [RIVERHOG_ARCHIVE_STORE_{store}_{setting}](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="MONTHLY_DOWNLOAD_ALLOWANCE_BYTES" |

## Governing policies

- <a id="pa-050785ff35"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-561ab94198"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../../../evidence/sources.md#src-3071ba44b3) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_archive_store_environment_name` |

### Machine authority

- `/external_contract/configuration_environment_patterns/0`

### Exact owned JSON

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
