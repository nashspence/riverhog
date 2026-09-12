# RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-range-billing-mode:08dc451511 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-80acbe8bb3"></a>
| Field | Shape |
|---|---|
| <a id="s-f3307f9b30"></a>`classification` | "runtime" |
| <a id="s-63d41d35b6"></a>`consumers` | ["riverhog-server"] |
| <a id="s-61d6785fe1"></a>`disposition` | "contractual" |
| <a id="s-6991f7b9b9"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE" |
| <a id="s-8091a081fc"></a>`name` | "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE" |
| <a id="s-cc5e961af0"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-b229f73afa"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../../../evidence/sources.md#src-d46028cd7f) — `riverhog/src/riverhog_core/pack_retrieval.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/pack_retrieval.py` | `_scoped_env_value(values, 'RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE', BILLING_MODE_RETURNED_BYTES, store_name=store_name)` |

### Machine authority

- `/external_contract/configuration_environment/75`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f06837250b4cd3183fe158123103aa32c0a81053ce4a37cabf037ffbc629955 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE",
  "name": "RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE",
  "owner": "riverhog-server"
}
```
