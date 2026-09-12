# configuration_environment_patterns-0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:configuration-environment-patterns-0:686eafb0a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [patterns](families/patterns/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-868df593f6"></a>
| Field | Shape |
|---|---|
| <a id="s-1898139272"></a>`consumer` | "riverhog-server" |
| <a id="s-84c52d0e1b"></a>`parameters` | additional keys=`setting`, `store` |
| <a id="s-e55cd62afd"></a>`template` | "RIVERHOG_ARCHIVE_STORE_{store}_{setting}" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [configuration_environment_patterns-0](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_MAX_CONNECTIONS" |
| [configuration_environment_patterns-0](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="ADAPTER_TIMEOUT_SECONDS" |
| [configuration_environment_patterns-0](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="DOWNLOAD_SAFETY_BUFFER_BYTES" |
| [configuration_environment_patterns-0](#s-868df593f6) | `value · configured-value · operational_policy` | configuration="MONTHLY_DOWNLOAD_ALLOWANCE_BYTES" |

## Governing policies

- <a id="pa-30e09366c4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-5856490288"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `scripts/contract_freeze.py::_environment_inventory`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment_patterns/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f14b45de57e080aadf202acdd7c5f105784c253ba0f80302df2b4893b01ede6 -->

```json
{
  "consumer": "riverhog-server",
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
