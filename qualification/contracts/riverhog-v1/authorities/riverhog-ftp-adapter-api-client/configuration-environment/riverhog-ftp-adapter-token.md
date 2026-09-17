# RIVERHOG_FTP_ADAPTER_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-token:7e77684aa6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-c4b672867c"></a>

| Field | Value |
|---|---|
| <a id="s-396c43f461"></a>`consumers` | `["riverhog-ftp-adapter-api-client"]` |
| <a id="s-d2f643cba8"></a>`default_expressions` | `["unset"]` |
| <a id="s-3f3f65b51c"></a>`id` | `"riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_TOKEN"` |
| <a id="s-c9113b6c41"></a>`input_shape` | `"environment-string"` |
| <a id="s-019de684d7"></a>`name` | `"RIVERHOG_FTP_ADAPTER_TOKEN"` |
| <a id="s-cd554621c2"></a>`owner` | `"riverhog-ftp-adapter-api-client"` |

## Governing policies

- <a id="pa-044bace8e9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_TOKEN](../../../evidence/sources.md#src-5a52b017b3) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py::RiverhogFtpAdapterClient.\_\_init\_\_](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter-api-client` | [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py) | `os.getenv('RIVERHOG_FTP_ADAPTER_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/31`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 920e01d3744b6157c872c4b2add2b5db846d03f0a3c937843f09ca00b6389367 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_TOKEN",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_TOKEN",
  "owner": "riverhog-ftp-adapter-api-client"
}
```

</details>
