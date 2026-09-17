# RIVERHOG_FTP_ADAPTER_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-base-url:6b86fbad10 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-d7a4e6e0e8"></a>

| Field | Value |
|---|---|
| <a id="s-a5e7a20adb"></a>`consumers` | `["riverhog-ftp-adapter-api-client"]` |
| <a id="s-d5c63849b4"></a>`default_expressions` | `["unset"]` |
| <a id="s-f2739e6aac"></a>`id` | `"riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_BASE_URL"` |
| <a id="s-7791533715"></a>`input_shape` | `"environment-string"` |
| <a id="s-495c4fd74d"></a>`name` | `"RIVERHOG_FTP_ADAPTER_BASE_URL"` |
| <a id="s-eba1397ffc"></a>`owner` | `"riverhog-ftp-adapter-api-client"` |

## Governing policies

- <a id="pa-44ffe61dbb"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_BASE_URL](../../../evidence/sources/authorities.md#src-aa9adbb30a) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py::RiverhogFtpAdapterClient.\_\_init\_\_](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter-api-client` | [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py) | `os.getenv('RIVERHOG_FTP_ADAPTER_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/28`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 148d6e94ae8967956c7cdea30bd1b0103214eb26b4d871f262e2669d6012f0f1 -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_BASE_URL",
  "owner": "riverhog-ftp-adapter-api-client"
}
```

</details>
