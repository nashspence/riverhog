# RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-allow-insecure-http:7637fdb663 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d99f18df3b"></a>
| Field | Shape |
|---|---|
| <a id="s-68e07c8d40"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-58d09e6295"></a>`default_expressions` | ["unset"] |
| <a id="s-c87e6808a4"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |
| <a id="s-fe1f42fe30"></a>`input_shape` | "environment-string" |
| <a id="s-7a39009700"></a>`name` | "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP" |
| <a id="s-32350f9b3a"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-08679344e3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-85f3c43f72) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 459df3bc4a8005d9caea51d73197b7bd75e9ca2e066ac4f8737dffacee0c450c -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
