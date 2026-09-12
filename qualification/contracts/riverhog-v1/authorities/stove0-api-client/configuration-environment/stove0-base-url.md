# STOVE0_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-api-client:stove0-base-url:ecd4942b8d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-8336665b97) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-205dff964b"></a>
| Field | Shape |
|---|---|
| <a id="s-5b2877eadc"></a>`classification` | "identity" |
| <a id="s-668719968c"></a>`consumers` | ["stove0-api-client"] |
| <a id="s-95c706e962"></a>`disposition` | "contractual" |
| <a id="s-8a0133ded5"></a>`id` | "stove0-api-client:environment:STOVE0_BASE_URL" |
| <a id="s-663968b1c7"></a>`name` | "STOVE0_BASE_URL" |
| <a id="s-d6a05173f7"></a>`owner` | "stove0-api-client" |

## Governing policies

- <a id="pa-dc30dbc23c"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:stove0-api-client:STOVE0_BASE_URL](../../../evidence/sources.md#src-34ec96f5f9) — `reference/stove0/packages/api-client/src/stove0_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/16/names` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `os.getenv('STOVE0_BASE_URL')` |
| parser | `stove0-api-client` | `reference/stove0/packages/api-client/src/stove0_api_client/client.py` | `safe_http_base_url(base_url or os.getenv('STOVE0_BASE_URL') or 'http://127.0.0.1:8080', setting='STOVE0_BASE_URL', allow_insecure_http=allow)` |

### Machine authority

- `/external_contract/configuration_environment/81`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39ba7f2ef2d3fb8ecea75ff4f0ff9f0aaeeb171e27f3666dacc499497ca24b18 -->

```json
{
  "classification": "identity",
  "consumers": [
    "stove0-api-client"
  ],
  "disposition": "contractual",
  "id": "stove0-api-client:environment:STOVE0_BASE_URL",
  "name": "STOVE0_BASE_URL",
  "owner": "stove0-api-client"
}
```
