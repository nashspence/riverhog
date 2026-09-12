# STOVE0_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-allow-insecure-http:4479c7e49f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-538df43105) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24fc48da99"></a>
| Field | Shape |
|---|---|
| <a id="s-82da5037a6"></a>`classification` | "runtime" |
| <a id="s-fa86d7a910"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-d1aff09a7b"></a>`disposition` | "contractual" |
| <a id="s-0452f51736"></a>`id` | "stove0-api-client:environment:STOVE0_ALLOW_INSECURE_HTTP" |
| <a id="s-071aa15ddd"></a>`name` | "STOVE0_ALLOW_INSECURE_HTTP" |
| <a id="s-40f3826282"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-7fda23d108"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-api-client:STOVE0_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-a9f40fdb75) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/17/names` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `_boolean_env('STOVE0_ALLOW_INSECURE_HTTP', False)` |

### Machine authority

- `/external_contract/configuration_environment/80`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f81fdac4a9154b2e04b3ea638416272356353d44f3644b425831c20ec54c8d24 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-api-client"
  ],
  "disposition": "contractual",
  "id": "stove0-api-client:environment:STOVE0_ALLOW_INSECURE_HTTP",
  "name": "STOVE0_ALLOW_INSECURE_HTTP",
  "owner": "stove0-api-client"
}
```
