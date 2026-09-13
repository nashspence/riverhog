# RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-store-setting:1fd115ebb2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [patterns](families/patterns/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-396301adfd"></a>
| Field | Shape |
|---|---|
| <a id="s-aa54bed389"></a>`consumers` | ["riverhog-server"] |
| <a id="s-cc204f154f"></a>`id` | "riverhog-server:environment-pattern:RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}" |
| <a id="s-08e772de25"></a>`input_shape` | "environment-string" |
| <a id="s-e74ff87647"></a>`owner` | "riverhog-server" |
| <a id="s-2a3ba4acfb"></a>`parameters` | additional keys=`setting`, `store` |
| <a id="s-1b3de31381"></a>`settings` | ["ADAPTER_URL","ADAPTER_TOKEN_FILE","ADAPTER_ALLOW_INSECURE_HTTP","ADAPTER_MAX_CONNECTIONS","ADAPTER_TIMEOUT_SECONDS","ADMISSION_ENABLED","ADMISSION_BUDGET_BYTES"] |
| <a id="s-29a999d8fe"></a>`source_symbol` | "_retrieval_cache_store_environment_name" |
| <a id="s-8821a6767d"></a>`template` | "RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: classification=null; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](#s-396301adfd) | `value · configured-value · operational_policy` | configuration="ADAPTER_MAX_CONNECTIONS" |
| [RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](#s-396301adfd) | `value · configured-value · operational_policy` | configuration="ADAPTER_TIMEOUT_SECONDS" |
| [RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](#s-396301adfd) | `value · configured-value · operational_policy` | configuration="ADMISSION_BUDGET_BYTES" |

## Governing policies

- <a id="pa-b43dad48d0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-dac115e962"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment-pattern:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](../../../evidence/sources.md#src-956dbca0d9) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `_retrieval_cache_store_environment_name` |

### Machine authority

- `/external_contract/configuration_environment_patterns/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40c685215b2b1f35200d0db0b8425a6dc21fc43d0d7cc820579b328c6a2615ef -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "id": "riverhog-server:environment-pattern:RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}",
  "input_shape": "environment-string",
  "owner": "riverhog-server",
  "parameters": {
    "setting": [
      "ADAPTER_URL",
      "ADAPTER_TOKEN_FILE",
      "ADAPTER_ALLOW_INSECURE_HTTP",
      "ADAPTER_MAX_CONNECTIONS",
      "ADAPTER_TIMEOUT_SECONDS",
      "ADMISSION_ENABLED",
      "ADMISSION_BUDGET_BYTES"
    ],
    "store": {
      "normalization": "uppercase-dashes-to-underscores",
      "source": "RIVERHOG_RETRIEVAL_CACHE_STORES"
    }
  },
  "settings": [
    "ADAPTER_URL",
    "ADAPTER_TOKEN_FILE",
    "ADAPTER_ALLOW_INSECURE_HTTP",
    "ADAPTER_MAX_CONNECTIONS",
    "ADAPTER_TIMEOUT_SECONDS",
    "ADMISSION_ENABLED",
    "ADMISSION_BUDGET_BYTES"
  ],
  "source_symbol": "_retrieval_cache_store_environment_name",
  "template": "RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}"
}
```
