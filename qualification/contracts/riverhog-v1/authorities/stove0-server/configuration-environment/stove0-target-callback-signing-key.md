# STOVE0_TARGET_CALLBACK_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-signing-key:27bc3f8210 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-182b18fbb1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4e217cc727"></a>
| Field | Shape |
|---|---|
| <a id="s-a3440faddf"></a>`classification` | "credential" |
| <a id="s-debc687946"></a>`consumers` | ["stove0-server"] |
| <a id="s-ebaf2f2e76"></a>`disposition` | "contractual" |
| <a id="s-27ea9cf097"></a>`id` | "stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY" |
| <a id="s-37722004b8"></a>`name` | "STOVE0_TARGET_CALLBACK_SIGNING_KEY" |
| <a id="s-3c2dd0740f"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-766a8d266d"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../../../evidence/sources.md#src-543ac98fff) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/26/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'STOVE0_TARGET_CALLBACK_SIGNING_KEY', required=bool(targets))` |

### Machine authority

- `/external_contract/configuration_environment/124`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9428cc70c710bdb1f0026552d77b9f5c7264ed11fdc6f4f92774d26f8819beb2 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY",
  "name": "STOVE0_TARGET_CALLBACK_SIGNING_KEY",
  "owner": "stove0-server"
}
```
