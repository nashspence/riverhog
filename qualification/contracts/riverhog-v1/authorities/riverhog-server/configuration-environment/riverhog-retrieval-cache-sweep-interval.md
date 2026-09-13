# RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-sweep-interval:3886780362 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-abc6c0ac51"></a>
| Field | Shape |
|---|---|
| <a id="s-1ed351fe80"></a>`consumers` | ["riverhog-server"] |
| <a id="s-942ac43e20"></a>`default_expressions` | ["'5m'"] |
| <a id="s-d14f4cb1fe"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |
| <a id="s-23ed4889af"></a>`input_shape` | "environment-string" |
| <a id="s-b3f8b0388d"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |
| <a id="s-92764138c3"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](#s-abc6c0ac51) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-a50799bba4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2ec725ac5e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../../../evidence/sources.md#src-d858623918) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL', '5m')` |

### Machine authority

- `/external_contract/configuration_environment/72`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19aff8b639cd317efb63416f413d438e181b54ed74680a0779bdd745280bff14 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'5m'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL",
  "owner": "riverhog-server"
}
```
