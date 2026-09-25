# A_RIVERHOG_FTP_SPOOL_HTTP2

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-http2:850cd9bdfc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-51bdf3ea73"></a>

| Field | Value |
|---|---|
| <a id="s-46d6397232"></a>`consumers` | `["a-riverhog-ftp-spool-client"]` |
| <a id="s-c720b22b45"></a>`default_expressions` | `["unset"]` |
| <a id="s-8708489666"></a>`id` | `"a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_HTTP2"` |
| <a id="s-0bb8695d4e"></a>`input_shape` | `"environment-string"` |
| <a id="s-cb037aed18"></a>`name` | `"A_RIVERHOG_FTP_SPOOL_HTTP2"` |
| <a id="s-869feebb7c"></a>`owner` | `"a-riverhog-ftp-spool-client"` |

## Governing policies

- <a id="pa-ce6eb575c5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool-client:A_RIVERHOG_FTP_SPOOL_HTTP2](../../../evidence/sources/authorities.md#src-7cadbc39d4) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::\_bool\_env](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool-client` | [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/60`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c60b4934e4faab1bf98fa2b26b72a6e662d28c4a3a4ffd021977e920aecafcd2 -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_HTTP2",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FTP_SPOOL_HTTP2",
  "owner": "a-riverhog-ftp-spool-client"
}
```

</details>
