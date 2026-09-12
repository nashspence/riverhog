# RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-max-inflight-bytes:4c80505b3a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-c992f01b6c"></a>
| Field | Shape |
|---|---|
| <a id="s-1ae3c070e9"></a>`classification` | "runtime" |
| <a id="s-ad7e41feee"></a>`consumers` | ["riverhog-server"] |
| <a id="s-ba3affbf74"></a>`disposition` | "contractual" |
| <a id="s-82330ee13f"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES" |
| <a id="s-4a3181eea5"></a>`name` | "RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES" |
| <a id="s-e9681c4a64"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](#s-c992f01b6c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-2fb0665ca1"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-b528b467f3"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../../../evidence/sources.md#src-cb29acf59b) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_bytes(values, 'RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES', DEFAULT_RETRIEVAL_MAX_INFLIGHT_BYTES)` |

### Machine authority

- `/external_contract/configuration_environment/71`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62ca065d3715dd96f7077fbbd4a91d7dcbe9b37bf15290e6a1813ff92724dce4 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "name": "RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES",
  "owner": "riverhog-server"
}
```
