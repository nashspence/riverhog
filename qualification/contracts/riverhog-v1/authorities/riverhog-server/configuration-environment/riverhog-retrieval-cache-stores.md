# RIVERHOG_RETRIEVAL_CACHE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-stores:2b91111628 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](families/settings/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c992f01b6c"></a>
| Field | Shape |
|---|---|
| <a id="s-ad7e41feee"></a>`consumers` | ["riverhog-server"] |
| <a id="s-3b77a1770c"></a>`default_expressions` | ["''"] |
| <a id="s-82330ee13f"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES" |
| <a id="s-15b98567cc"></a>`input_shape` | "environment-string" |
| <a id="s-4a3181eea5"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_STORES" |
| <a id="s-e9681c4a64"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-d664db62e8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_STORES](../../../evidence/sources.md#src-51dd06072e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_RETRIEVAL_CACHE_STORES', '')` |

### Machine authority

- `/external_contract/configuration_environment/71`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b646dd10a847f539b88e5724174ffdbe664941b2e0a5523a426f573318a16f7a -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES",
  "input_shape": "environment-string",
  "name": "RIVERHOG_RETRIEVAL_CACHE_STORES",
  "owner": "riverhog-server"
}
```
