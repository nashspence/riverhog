# RIVERHOG_RETRIEVAL_CACHE_STORES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-stores:ae3edf470a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](families/identity/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-90cd77baf7"></a>
| Field | Shape |
|---|---|
| <a id="s-a8f6876fb2"></a>`classification` | "identity" |
| <a id="s-bc48d55b1e"></a>`consumers` | ["riverhog-server"] |
| <a id="s-a0e6863334"></a>`disposition` | "contractual" |
| <a id="s-48764ad0e2"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES" |
| <a id="s-35c021851e"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_STORES" |
| <a id="s-5bf462d167"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-d7a96926d8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_STORES](../../../evidence/sources.md#src-51dd06072e) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/13/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `values.get('RIVERHOG_RETRIEVAL_CACHE_STORES', '')` |

### Machine authority

- `/external_contract/configuration_environment/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74e9fb400bbc727c99b07bd17cf620c01884be42b6b650073c767382e785f534 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_STORES",
  "name": "RIVERHOG_RETRIEVAL_CACHE_STORES",
  "owner": "riverhog-server"
}
```
