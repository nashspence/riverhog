# RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-http-timeout-seconds:2f13ff852c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-2d908ff8df"></a>

| Field | Value |
|---|---|
| <a id="s-c799298a70"></a>`consumers` | `["riverhog-ftp-adapter-api-client"]` |
| <a id="s-539ad8b835"></a>`default_expressions` | `["unset"]` |
| <a id="s-69dffd9a14"></a>`id` | `"riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-f8a11ba5cc"></a>`input_shape` | `"environment-string"` |
| <a id="s-c530fa2f45"></a>`name` | `"RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-c6f518f261"></a>`owner` | `"riverhog-ftp-adapter-api-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS"; consumers=["riverhog-ftp-adapter-api-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](#s-2d908ff8df) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-8f962e626c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-84b86f49ce"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-97bfcc2b77) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py::\_timeout\_env](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter-api-client` | [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/30`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1b6185fa5ceb7164076a6578a363f3c3a0a518deda40092b240af9bf1faad3cc -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter-api-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-ftp-adapter-api-client:environment:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS",
  "owner": "riverhog-ftp-adapter-api-client"
}
```

</details>
