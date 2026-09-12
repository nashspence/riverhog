# STOVE0_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-database-url:710b76aa32 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-182b18fbb1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0cc068cac2"></a>
| Field | Shape |
|---|---|
| <a id="s-ef39b156db"></a>`classification` | "credential" |
| <a id="s-ba1e600e45"></a>`consumers` | ["stove0-server"] |
| <a id="s-3efbc656f3"></a>`disposition` | "contractual" |
| <a id="s-d5be6be860"></a>`id` | "stove0-server:environment:STOVE0_DATABASE_URL" |
| <a id="s-d7bcf43917"></a>`name` | "STOVE0_DATABASE_URL" |
| <a id="s-c095dc5b0b"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-6c467eac51"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_DATABASE_URL](../../../evidence/sources.md#src-d05255fc63) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/26/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'STOVE0_DATABASE_URL', required=True)` |

### Machine authority

- `/external_contract/configuration_environment/115`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4473bb0f3844e8463e7336fb4f91ead30633c6fcfa7023873d72cfd07512156e -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_DATABASE_URL",
  "name": "STOVE0_DATABASE_URL",
  "owner": "stove0-server"
}
```
