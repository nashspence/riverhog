# STOVE0_BROWSE_TOKEN_SIGNING_KEY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-browse-token-signing-key:be1e2eafcc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-182b18fbb1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a5bdad2233"></a>
| Field | Shape |
|---|---|
| <a id="s-527a3da060"></a>`classification` | "credential" |
| <a id="s-ee93841a02"></a>`consumers` | ["stove0-server"] |
| <a id="s-2f60393843"></a>`disposition` | "contractual" |
| <a id="s-1eb66f737d"></a>`id` | "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-4ca352f816"></a>`name` | "STOVE0_BROWSE_TOKEN_SIGNING_KEY" |
| <a id="s-2bd29e5838"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-d9abc45e57"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../../../evidence/sources.md#src-f552366814) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/26/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'STOVE0_BROWSE_TOKEN_SIGNING_KEY', required=True)` |

### Machine authority

- `/external_contract/configuration_environment/112`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7ea3241bd51e115257456af6e370e251cc198ba8454faa7a4f241075cb902a6 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "name": "STOVE0_BROWSE_TOKEN_SIGNING_KEY",
  "owner": "stove0-server"
}
```
