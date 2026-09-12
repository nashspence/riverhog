# RIVERHOG_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-client:riverhog-token:316216d17a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [credential](index.md#f-14bacbbb18) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9de9805f44"></a>
| Field | Shape |
|---|---|
| <a id="s-9250b9918c"></a>`classification` | "credential" |
| <a id="s-f0e22869ab"></a>`consumers` | ["riverhog-client"] |
| <a id="s-260932a95a"></a>`disposition` | "contractual" |
| <a id="s-7c3da5fa0c"></a>`id` | "riverhog-client:environment:RIVERHOG_TOKEN" |
| <a id="s-81fae89078"></a>`name` | "RIVERHOG_TOKEN" |
| <a id="s-683687933c"></a>`owner` | "riverhog-client" |

## Governing policies

- <a id="pa-65d6cb07b5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-client:RIVERHOG_TOKEN](../../../evidence/sources.md#src-cc55a54933) — `packages/riverhog-client/src/riverhog_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/3/names` |
| parser | `riverhog-client` | `packages/riverhog-client/src/riverhog_client/client.py` | `super().__init__(base_url, token, token_env='RIVERHOG_TOKEN', allow_insecure_http=allow_insecure_http)` |

### Machine authority

- `/external_contract/configuration_environment/17`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd224d8965a0cca38dad1c32d3e5dec0ba9dbc1b7730e3a5e3e0c4062ffc4b7d -->

```json
{
  "classification": "credential",
  "consumers": [
    "riverhog-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-client:environment:RIVERHOG_TOKEN",
  "name": "RIVERHOG_TOKEN",
  "owner": "riverhog-client"
}
```
