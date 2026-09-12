# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-allow-insecure-http:8a5f8d282d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-e4b312fabf) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e342da9aef"></a>
| Field | Shape |
|---|---|
| <a id="s-841e5c023a"></a>`classification` | "runtime" |
| <a id="s-1e916619c8"></a>`consumers` | ["riverhog-client"] |
| <a id="s-a8aab9431f"></a>`disposition` | "contractual" |
| <a id="s-6907510a84"></a>`id` | "riverhog-client:environment:RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-89688e57e4"></a>`name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-b6d0b64384"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-d273c3dd92"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-6501ce08a3) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/5/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `_bool_env('RIVERHOG_ALLOW_INSECURE_HTTP', False)` |

### Machine authority

- `/external_contract/configuration_environment/9`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 384fbccdd4ebed5dba19caafe9c587b991311ec3bf1824603d2087228c2d94f2 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-client"
}
```
