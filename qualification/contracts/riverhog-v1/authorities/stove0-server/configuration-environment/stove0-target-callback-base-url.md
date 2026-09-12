# STOVE0_TARGET_CALLBACK_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-base-url:869b8348e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f3dc096fc3"></a>
| Field | Shape |
|---|---|
| <a id="s-a8c92dd5e7"></a>`classification` | "identity" |
| <a id="s-89ecd9cd3b"></a>`consumers` | ["stove0-server"] |
| <a id="s-0ba62b1388"></a>`disposition` | "contractual" |
| <a id="s-201310b467"></a>`id` | "stove0-server:environment:STOVE0_TARGET_CALLBACK_BASE_URL" |
| <a id="s-4baab6d3a2"></a>`name` | "STOVE0_TARGET_CALLBACK_BASE_URL" |
| <a id="s-7580b0bc08"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-5037f69078"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_BASE_URL](../../../evidence/sources.md#src-074108cdde) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get('STOVE0_TARGET_CALLBACK_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/123`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 363428f85def8cb7a7afadf58875988bca9fdc26eb4abe188cf12e542cf98c11 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_BASE_URL",
  "name": "STOVE0_TARGET_CALLBACK_BASE_URL",
  "owner": "stove0-server"
}
```
