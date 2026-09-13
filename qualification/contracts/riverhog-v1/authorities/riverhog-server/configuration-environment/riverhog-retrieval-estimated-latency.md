# RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-estimated-latency:5916c51f2f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-80acbe8bb3"></a>
| Field | Shape |
|---|---|
| <a id="s-63d41d35b6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-698b9afc79"></a>`default_expressions` | ["'48h'"] |
| <a id="s-6991f7b9b9"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |
| <a id="s-0783a8b801"></a>`input_shape` | "environment-string" |
| <a id="s-8091a081fc"></a>`name` | "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY" |
| <a id="s-cc5e961af0"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-e3951f0544"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../../../evidence/sources.md#src-63c6f042b8) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY', '48h')` |

### Machine authority

- `/external_contract/configuration_environment/75`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c15b27ca1c8d6f3f14933e08353cbeeeb2e0d21c9291149054a6a11a7b348ec -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'48h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY",
  "owner": "riverhog-server"
}
```
