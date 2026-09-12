# RIVERHOG_FTP_ADAPTER_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-http2:424e3f78d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-1b544c4817) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0ef9db2534"></a>
| Field | Shape |
|---|---|
| <a id="s-0a3d779fd7"></a>`classification` | "runtime" |
| <a id="s-3d50fc447d"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-ac5987771e"></a>`disposition` | "contractual" |
| <a id="s-35f99da18f"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP2" |
| <a id="s-8eed53d4d3"></a>`name` | "RIVERHOG_FTP_ADAPTER_HTTP2" |
| <a id="s-282ecf4905"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-2cc754f46b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP2](../../../evidence/sources.md#src-dd720ed781) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/10/names` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `_bool_env('RIVERHOG_FTP_ADAPTER_HTTP2', True)` |

### Machine authority

- `/external_contract/configuration_environment/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75123a8babcaac95c9d917222eadd7cfafb2c4d17793924ee85b33171e08bb01 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP2",
  "name": "RIVERHOG_FTP_ADAPTER_HTTP2",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
