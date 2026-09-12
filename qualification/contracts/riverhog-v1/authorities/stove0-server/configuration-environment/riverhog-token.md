# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-token:6cb21f9514 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-182b18fbb1) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8af4063a2b"></a>
| Field | Shape |
|---|---|
| <a id="s-ce2b2eef79"></a>`classification` | "credential" |
| <a id="s-780fea1d0c"></a>`consumers` | ["stove0-server"] |
| <a id="s-e236325de3"></a>`disposition` | "contractual" |
| <a id="s-2760bd28e2"></a>`id` | "stove0-server:environment:RIVERHOG_TOKEN" |
| <a id="s-9feb87a0c4"></a>`name` | "RIVERHOG_TOKEN" |
| <a id="s-095861fb48"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-b69afa86ae"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:RIVERHOG_TOKEN](../../../evidence/sources.md#src-a2d6243ee8) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/26/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_secret(values, 'RIVERHOG_TOKEN', required=True)` |

### Machine authority

- `/external_contract/configuration_environment/108`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebbb688c7766ae2e4f44527464f94723a83e0ee408e436aadb9a82a8c1499d1c -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:RIVERHOG_TOKEN",
  "name": "RIVERHOG_TOKEN",
  "owner": "stove0-server"
}
```
