# RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-http-timeout-seconds:72dac41fbc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [runtime](index.md#f-1b544c4817) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-d99f18df3b"></a>
| Field | Shape |
|---|---|
| <a id="s-6c93399da9"></a>`classification` | "runtime" |
| <a id="s-68e07c8d40"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-6727008617"></a>`disposition` | "contractual" |
| <a id="s-c87e6808a4"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS" |
| <a id="s-7a39009700"></a>`name` | "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS" |
| <a id="s-32350f9b3a"></a>`owner` | "riverhog-ftp-adapter-api-client" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"; consumers=["riverhog-ftp-adapter-api-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](#s-d99f18df3b) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-091cce27fb"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-2f9744487e"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:inventory](../../../evidence/sources.md#src-26b33461d2) — `qualification/configuration-contract.toml`
- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../../../evidence/sources.md#src-97bfcc2b77) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The declaration fixes normative ownership and classification. The parser expression is the source-linked authority for the accepted domain and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| declaration | — | `qualification/configuration-contract.toml` | `/environment/10/names` |
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `_timeout_env('RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS', 300.0)` |

### Machine authority

- `/external_contract/configuration_environment/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3577f2a992fa5b2c4eb5c66fbe68697bc4c2775aab2f375e5481f52c88b5ac24 -->

```json
{
  "classification": "runtime",
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "disposition": "contractual",
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS",
  "name": "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
