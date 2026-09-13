# RIVERHOG_FTP_ADAPTER_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-http2:c282548fbe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-eb2021dc66"></a>
| Field | Shape |
|---|---|
| <a id="s-f72318c871"></a>`consumers` | ["riverhog-ftp-adapter-api-client"] |
| <a id="s-c5edc5ccb1"></a>`default_expressions` | ["unset"] |
| <a id="s-f6f255e1f5"></a>`id` | "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP2" |
| <a id="s-546d76419e"></a>`input_shape` | "environment-string" |
| <a id="s-b4738a40ad"></a>`name` | "RIVERHOG_FTP_ADAPTER_HTTP2" |
| <a id="s-407a9add94"></a>`owner` | "riverhog-ftp-adapter-api-client" |

## Governing policies

- <a id="pa-4093120dd7"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP2](../../../evidence/sources.md#src-dd720ed781) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter-api-client` | `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py` | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/29`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fb4dd8dbb59604c1a1fd57d2a44c5e71e5554b71d5ec6a543c4d9d48e83717b -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP2",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_HTTP2",
  "owner": "riverhog-ftp-adapter-api-client"
}
```
