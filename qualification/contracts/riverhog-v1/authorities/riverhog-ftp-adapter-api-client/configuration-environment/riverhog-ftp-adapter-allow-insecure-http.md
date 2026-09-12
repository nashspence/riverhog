# RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-allow-insecure-http:e64ae7ddd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-1b544c4817) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-da42edbdae"></a>
| Field | Shape |
|---|---|
| <a id="s-cdcd303009"></a>`classification` | "runtime" |
| <a id="s-cb8b3d3b8e"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-b75675235a"></a>`disposition` | "contractual" |
| <a id="s-c407419be0"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |
| <a id="s-c08568c77a"></a>`name` | "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |
| <a id="s-bf0770eba1"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-d9ad708cd5"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-85f3c43f72) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/10/names` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `_bool_env('RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP', False)` |

### Machine authority

- `/external_contract/configuration_environment/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c8c5f5a61f56e03d433dc2d0cb1f56b755bd2beba4b75915a588d19b92b3469 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP",
  "name": "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
