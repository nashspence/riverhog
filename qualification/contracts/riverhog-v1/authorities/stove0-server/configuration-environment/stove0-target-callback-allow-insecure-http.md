# STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-allow-insecure-http:d3ed6adc78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1c81252996"></a>
| Field | Shape |
|---|---|
| <a id="s-0430c524eb"></a>`classification` | "runtime" |
| <a id="s-841c1e0c0e"></a>`consumers` | ["stove0-server"] |
| <a id="s-7236b9eeb0"></a>`disposition` | "contractual" |
| <a id="s-ae1c3e3714"></a>`id` | "stove0-server:environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |
| <a id="s-cfc1e16201"></a>`name` | "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |
| <a id="s-f76569877b"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-0c2f39a764"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-00eaae2681) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_boolean(values, 'STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP', False)` |

### Machine authority

- `/external_contract/configuration_environment/122`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f88f9a801101ce433738a61eeb1d7987ec1cda796bfd867d46fa027e398bce2 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP",
  "name": "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP",
  "owner": "stove0-server"
}
```
