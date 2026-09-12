# STOVE0_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http2:bad59327e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-538df43105) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-cbedc559e6"></a>
| Field | Shape |
|---|---|
| <a id="s-f15fa35984"></a>`classification` | "runtime" |
| <a id="s-339b989f89"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-63c375be9c"></a>`disposition` | "contractual" |
| <a id="s-ab96b2e072"></a>`id` | "stove0-api-client:environment:STOVE0_HTTP2" |
| <a id="s-7dead87c9e"></a>`name` | "STOVE0_HTTP2" |
| <a id="s-a99760a4aa"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-de63560346"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-api-client:STOVE0_HTTP2](../../../evidence/sources.md#src-29a943f529) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/17/names` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `_boolean_env('STOVE0_HTTP2', True)` |

### Machine authority

- `/external_contract/configuration_environment/82`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e8724bb1809ee067b2e89a9b450a938b0b02c810d8984b5fa50514c3e4c4ccb -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-api-client"
  ],
  "disposition": "contractual",
  "id": "stove0-api-client:environment:STOVE0_HTTP2",
  "name": "STOVE0_HTTP2",
  "owner": "stove0-api-client"
}
```
