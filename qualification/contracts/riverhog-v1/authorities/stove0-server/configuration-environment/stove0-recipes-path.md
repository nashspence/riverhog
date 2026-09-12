# STOVE0_RECIPES_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-recipes-path:b8b36d10b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f0ba6e17dd"></a>
| Field | Shape |
|---|---|
| <a id="s-a3fb51fc29"></a>`classification` | "identity" |
| <a id="s-9c7aa095b0"></a>`consumers` | ["stove0-server"] |
| <a id="s-b18b8c89ec"></a>`disposition` | "contractual" |
| <a id="s-bc0136d5ee"></a>`id` | "stove0-server:environment:STOVE0_RECIPES_PATH" |
| <a id="s-3e59f086de"></a>`name` | "STOVE0_RECIPES_PATH" |
| <a id="s-f783132f7f"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-f375d93c0f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_RECIPES_PATH](../../../evidence/sources.md#src-afbf7c2b57) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_required(values, 'STOVE0_RECIPES_PATH')` |

### Machine authority

- `/external_contract/configuration_environment/118`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea967a203e0ea1b4c4091450bf1ef0327c144ab2436c128ef0f8185cebf6bd81 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_RECIPES_PATH",
  "name": "STOVE0_RECIPES_PATH",
  "owner": "stove0-server"
}
```
