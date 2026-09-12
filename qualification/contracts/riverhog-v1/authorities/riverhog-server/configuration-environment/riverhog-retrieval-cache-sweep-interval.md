# RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-cache-sweep-interval:a4f82c2f77 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-a44ff15aa7"></a>
| Field | Shape |
|---|---|
| <a id="s-539f60852a"></a>`classification` | "runtime" |
| <a id="s-cbfaedd8f7"></a>`consumers` | ["riverhog-server"] |
| <a id="s-6159b0cd74"></a>`disposition` | "contractual" |
| <a id="s-2908730b97"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |
| <a id="s-394d41cd98"></a>`name` | "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL" |
| <a id="s-436907c2b0"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](#s-a44ff15aa7) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-7b80c0f1f1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-36ac77b607"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../../../evidence/sources.md#src-d858623918) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL', '5m')` |

### Machine authority

- `/external_contract/configuration_environment/67`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07b5081d5360952a06742abfa191777b8b6b1fb4d5299d6313dd01468004a3f3 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL",
  "name": "RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL",
  "owner": "riverhog-server"
}
```
