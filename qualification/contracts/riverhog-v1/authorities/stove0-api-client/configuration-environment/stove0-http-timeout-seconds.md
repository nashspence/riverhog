# STOVE0_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-http-timeout-seconds:cdbaee86be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-538df43105) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-658650862c"></a>
| Field | Shape |
|---|---|
| <a id="s-58ddaddc90"></a>`classification` | "runtime" |
| <a id="s-e8619b82d7"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-5eff1bd53b"></a>`disposition` | "contractual" |
| <a id="s-3dd36fbb03"></a>`id` | "stove0-api-client:environment:STOVE0_HTTP_TIMEOUT_SECONDS" |
| <a id="s-dc1bc511db"></a>`name` | "STOVE0_HTTP_TIMEOUT_SECONDS" |
| <a id="s-6010d735e6"></a>`owner` | "stove0-api-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="STOVE0_HTTP_TIMEOUT_SECONDS"; consumers=["stove0-api-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [STOVE0_HTTP_TIMEOUT_SECONDS](#s-658650862c) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-4b33b846ec"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-8a3b9a9a20"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-a470f49fc7) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/17/names` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `_positive_float_env('STOVE0_HTTP_TIMEOUT_SECONDS', 300.0)` |

### Machine authority

- `/external_contract/configuration_environment/83`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68b4e9f79fc9f94111dec0331d2e28689f3d8df37c7384a1cf408db7b6d6d628 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "stove0-api-client"
  ],
  "disposition": "contractual",
  "id": "stove0-api-client:environment:STOVE0_HTTP_TIMEOUT_SECONDS",
  "name": "STOVE0_HTTP_TIMEOUT_SECONDS",
  "owner": "stove0-api-client"
}
```
