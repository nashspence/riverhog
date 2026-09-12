# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-allow-insecure-http:271bf76265 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-2da04c48df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-62bc3f7cc9"></a>
| Field | Shape |
|---|---|
| <a id="s-4b56d7d465"></a>`classification` | "runtime" |
| <a id="s-119f21dafe"></a>`consumers` | ["stove0-server"] |
| <a id="s-d0d24db46e"></a>`disposition` | "contractual" |
| <a id="s-01069a2d81"></a>`id` | "stove0-server:environment:RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-02fadb9af0"></a>`name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-6ab5fa12ac"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-88cd807004"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-ab009ffd92) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/28/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_boolean(values, 'RIVERHOG_ALLOW_INSECURE_HTTP', False)` |

### Machine authority

- `/external_contract/configuration_environment/106`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03da4c949f08568a75043902c2345cad011d417a1f562e355fddeabaf63db769 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "stove0-server"
}
```
