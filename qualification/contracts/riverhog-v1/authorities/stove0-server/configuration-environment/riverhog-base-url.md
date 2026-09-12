# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:riverhog-base-url:2e07f0e4a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-d2342f695d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b002419278"></a>
| Field | Shape |
|---|---|
| <a id="s-0cf0cc4d5d"></a>`classification` | "identity" |
| <a id="s-e3ac27b7d7"></a>`consumers` | ["stove0-server"] |
| <a id="s-e55194de44"></a>`disposition` | "contractual" |
| <a id="s-42e9fbb861"></a>`id` | "stove0-server:environment:RIVERHOG_BASE_URL" |
| <a id="s-058d8b8874"></a>`name` | "RIVERHOG_BASE_URL" |
| <a id="s-46978b3ca6"></a>`owner` | "stove0-server" |

## Governing policies

- <a id="pa-49d36c6975"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-server:RIVERHOG_BASE_URL](../../../evidence/sources.md#src-94aba7e378) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/27/names` |
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `_required(values, 'RIVERHOG_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/107`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 45315ce8bc7a72aaf7eb58ce9f4ce47ae105a4f9652ef6fccc9e390701db1553 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-server"
  ],
  "disposition": "contractual",
  "id": "stove0-server:environment:RIVERHOG_BASE_URL",
  "name": "RIVERHOG_BASE_URL",
  "owner": "stove0-server"
}
```
