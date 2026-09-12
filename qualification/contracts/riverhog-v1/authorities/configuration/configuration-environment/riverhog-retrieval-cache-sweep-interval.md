# RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:riverhog-retrieval-cache-sweep-interval:ddd4870015 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a9033edf7a"></a>
| Field | Shape |
|---|---|
| <a id="s-7c2ffa6bcc"></a>`consumers` | ["riverhog-server"] |
| <a id="s-7e4a9547db"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](#s-a9033edf7a) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-66f995525f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-df93521891"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../../../evidence/sources.md#src-eefaf953b4) — `configuration-environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/61`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2402ae87bc61e0e0d06dfd760dd97369473b1086c7530b778fa79826c5b185f3 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "name": "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL"
}
```
