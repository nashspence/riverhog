# RIVERHOG_FTP_ADAPTER_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-base-url:74c0e6a52a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [identity](index.md#f-7063261835) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1b7215e251"></a>
| Field | Shape |
|---|---|
| <a id="s-2398700941"></a>`classification` | "identity" |
| <a id="s-84c48e5200"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-cd12feead7"></a>`disposition` | "contractual" |
| <a id="s-11b04fc0e6"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_BASE_URL" |
| <a id="s-399d989cc4"></a>`name` | "RIVERHOG_FTP_ADAPTER_BASE_URL" |
| <a id="s-03a4e23000"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-da3601dd5e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_BASE_URL](../../../evidence/sources.md#src-aa9adbb30a) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/9/names` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `os.getenv('RIVERHOG_FTP_ADAPTER_BASE_URL')` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `safe_http_base_url(base_url or os.getenv('RIVERHOG_FTP_ADAPTER_BASE_URL') or 'http://127.0.0.1:8082', setting='RIVERHOG_FTP_ADAPTER_BASE_URL', allow_insecure_http=allow)` |

### Machine authority

- `/external_contract/configuration_environment/25`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25e378d2eb7e77fbc8a6909b814e5a4cb00e5b60722f0cde745466faddfaaf98 -->

```json
{
  "classification": "identity",
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_BASE_URL",
  "name": "RIVERHOG_FTP_ADAPTER_BASE_URL",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
