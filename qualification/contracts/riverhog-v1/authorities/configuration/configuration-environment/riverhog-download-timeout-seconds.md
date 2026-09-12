# RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-download-timeout-seconds:5868d6be91 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1c3c820c2c"></a>
| Field | Shape |
|---|---|
| <a id="s-aca3251563"></a>`consumers` | ["riverhog-client"] |
| <a id="s-5945522bb9"></a>`name` | "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](#s-1c3c820c2c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-6ce73cb047"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-a875e90bcd"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../../../evidence/sources.md#src-ef9582fda4) — `configuration-environment:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/36`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ad4f976d6c4931ba2cb4ee7ac1c7d653aeba56c71bbf9792aed72c023781774 -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS"
}
```
