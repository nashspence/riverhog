# RIVERHOG_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-http-timeout-seconds:d0a1512599 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-6d67d56ce0b1"></a>
| Field | Shape |
|---|---|
| <a id="s-1a3e8ba5014a"></a>`consumers` | ["riverhog-client"] |
| <a id="s-639a4bef7fee"></a>`name` | "RIVERHOG_HTTP_TIMEOUT_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="RIVERHOG_HTTP_TIMEOUT_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_HTTP_TIMEOUT_SECONDS](#s-6d67d56ce0b1) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-d995269b75e6"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-950c310b1503"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-52020dc34588) — `configuration-environment:RIVERHOG_HTTP_TIMEOUT_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6283c7fc7becef59b0b0aa64a9a94ddcaff6f7ec6deacba82cda6295d05221f -->

```json
{
  "consumers": [
    "riverhog-client"
  ],
  "name": "RIVERHOG_HTTP_TIMEOUT_SECONDS"
}
```
