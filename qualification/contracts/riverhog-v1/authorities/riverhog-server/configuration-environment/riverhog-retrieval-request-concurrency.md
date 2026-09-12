# RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-retrieval-request-concurrency:3f61faa7d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](families/runtime/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-3efbdd0d50"></a>
| Field | Shape |
|---|---|
| <a id="s-eaaa53d45f"></a>`classification` | "runtime" |
| <a id="s-e5a8456801"></a>`consumers` | ["riverhog-server"] |
| <a id="s-5edfa5a885"></a>`disposition` | "contractual" |
| <a id="s-c4e21ab4c8"></a>`id` | "riverhog-server:environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY" |
| <a id="s-8bf9d9fca2"></a>`name` | "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY" |
| <a id="s-33c76d2bc9"></a>`owner` | "riverhog-server" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY"; consumers=["riverhog-server"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](#s-3efbdd0d50) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4cb7159ed7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-1dcdee2da1"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../../../evidence/sources.md#src-a586c2cf3e) — `riverhog/src/riverhog_core/throughput.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/14/names` |
| parser | `riverhog-server` | `riverhog/src/riverhog_core/throughput.py` | `_env_int(values, 'RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY', DEFAULT_RETRIEVAL_REQUEST_CONCURRENCY)` |

### Machine authority

- `/external_contract/configuration_environment/78`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46528f8e2dbd6d3bdccc544acbc3ffbd76020259744a6b8f3a98b486dc1bc7e6 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-server"
  ],
  "disposition": "contractual",
  "id": "riverhog-server:environment:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY",
  "name": "RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY",
  "owner": "riverhog-server"
}
```
