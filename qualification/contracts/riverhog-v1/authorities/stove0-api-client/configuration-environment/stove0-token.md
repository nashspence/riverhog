# STOVE0_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-token:43e0917537 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-c08977de6d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-47fb606ae5"></a>
| Field | Shape |
|---|---|
| <a id="s-1bb6ee57c8"></a>`classification` | "credential" |
| <a id="s-c114306576"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-67937135aa"></a>`disposition` | "contractual" |
| <a id="s-8f3b8fe1e3"></a>`id` | "stove0-api-client:environment:STOVE0_TOKEN" |
| <a id="s-e3159c3eeb"></a>`name` | "STOVE0_TOKEN" |
| <a id="s-065409c482"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-48a92b5533"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-api-client:STOVE0_TOKEN](../../../evidence/sources.md#src-c052f8f5ca) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/15/names` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `os.getenv('STOVE0_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/84`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f5f48f550915a0b53c4fc0318623abe43798cdfc1ea25bc1226308b85e01a42 -->

```json
{
  "classification": "credential",
  "consumers": [
    "stove0-api-client"
  ],
  "disposition": "contractual",
  "id": "stove0-api-client:environment:STOVE0_TOKEN",
  "name": "STOVE0_TOKEN",
  "owner": "stove0-api-client"
}
```
