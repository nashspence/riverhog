# STOVE0_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-http-timeout-seconds:b4e1c3045c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-5f56b9ebf5"></a>
| Field | Shape |
|---|---|
| <a id="s-451b5d705f"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-7dbd023a7b"></a>`name` | "STOVE0_HTTP_TIMEOUT_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_HTTP_TIMEOUT_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_HTTP_TIMEOUT_SECONDS](#s-5f56b9ebf5) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-2bebb8c5c4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b8aa74d19a"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-e0ab08e60c) — `configuration-environment:STOVE0_HTTP_TIMEOUT_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/105`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cce47c4651b931fb44e14227aacc1fc97bd7af7f5fb6329d9b1cf19ef9031b6 -->

```json
{
  "consumers": [
    "stove0-api-client"
  ],
  "name": "STOVE0_HTTP_TIMEOUT_SECONDS"
}
```
