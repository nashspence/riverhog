# RIVERHOG_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-http2:57431041d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1ebfad79c3"></a>
| Field | Shape |
|---|---|
| <a id="s-3e5887eac5"></a>`classification` | "runtime" |
| <a id="s-42ed9823cb"></a>`consumers` | ["riverhog-client"] |
| <a id="s-cece923a6f"></a>`disposition` | "contractual" |
| <a id="s-27fcc796bf"></a>`id` | "riverhog-client:environment:RIVERHOG_HTTP2" |
| <a id="s-def2479a70"></a>`name` | "RIVERHOG_HTTP2" |
| <a id="s-18f1726a77"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-469da6601b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_HTTP2](../../../evidence/sources.md#src-1774dabb11) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `_bool_env('RIVERHOG_HTTP2', True)` |

### Machine authority

- `/external_contract/configuration_environment/15`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ee50895295fb52bde755a329ccebd0b11bb1ad5d48573f5303f96eff2ac2bcd -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_HTTP2",
  "name": "RIVERHOG_HTTP2",
  "owner": "riverhog-client"
}
```
