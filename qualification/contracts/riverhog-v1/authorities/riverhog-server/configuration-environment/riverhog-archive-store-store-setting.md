# RIVERHOG_ARCHIVE_STORE_{store}_{setting}

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-archive-store-store-setting:8fd4fc93c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [patterns](families/patterns/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-868df593f6"></a>
| Field | Shape |
|---|---|
| <a id="s-90af33dd67"></a>`classifications` | additional keys=`ADAPTER_ALLOW_INSECURE_HTTP`, `ADAPTER_MAX_CONNECTIONS`, `ADAPTER_TIMEOUT_SECONDS`, `ADAPTER_TOKEN_FILE`, `ADAPTER_URL`, `DOWNLOAD_SAFETY_BUFFER_BYTES`, `MONTHLY_DOWNLOAD_ALLOWANCE_BYTES` |
| <a id="s-20593819b0"></a>`consumers` | ["riverhog-server"] |
| <a id="s-b226e6cdee"></a>`disposition` | "contractual" |
| <a id="s-d00519f807"></a>`id` | "riverhog-server:environment-pattern:RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |
| <a id="s-d1cd0ff44c"></a>`owner` | "riverhog-server" |
| <a id="s-84c52d0e1b"></a>`parameters` | additional keys=`setting`, `store` |
| <a id="s-e55cd62afd"></a>`template` | "RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: classification="runtime"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

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
- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment_pattern/0` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `ARCHIVE_STORE_ENVIRONMENT_TEMPLATE and ARCHIVE_STORE_ENVIRONMENT_SETTINGS` |

### Machine authority

- `/external_contract/configuration_environment_patterns/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 050700ae9a296d8138337ce9ed60e8a389d11262bed489cd3004ad2e09eb2dad -->

```json
{
  "classifications": {
    "ADAPTER_ALLOW_INSECURE_HTTP": "runtime",
    "ADAPTER_MAX_CONNECTIONS": "runtime",
    "ADAPTER_TIMEOUT_SECONDS": "runtime",
    "ADAPTER_TOKEN_FILE": "credential",
    "ADAPTER_URL": "identity",
    "DOWNLOAD_SAFETY_BUFFER_BYTES": "runtime",
    "MONTHLY_DOWNLOAD_ALLOWANCE_BYTES": "runtime"
  },
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment-pattern:RIVERHOG_ARCHIVE_STORE_{store}_{setting}",
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
  "template": "RIVERHOG_ARCHIVE_STORE_{store}_{setting}"
}
```
