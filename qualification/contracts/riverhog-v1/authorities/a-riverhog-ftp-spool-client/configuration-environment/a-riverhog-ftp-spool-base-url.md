# A_RIVERHOG_FTP_SPOOL_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-base-url:c9eb198bb2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-749bd6e141"></a>

| Field | Value |
|---|---|
| <a id="s-032458916b"></a>`consumers` | `["a-riverhog-ftp-spool-client"]` |
| <a id="s-595d4c8a28"></a>`default_expressions` | `["unset"]` |
| <a id="s-6b131cbb88"></a>`id` | `"a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_BASE_URL"` |
| <a id="s-deda87c07c"></a>`input_shape` | `"environment-string"` |
| <a id="s-94ad8621b7"></a>`name` | `"A_RIVERHOG_FTP_SPOOL_BASE_URL"` |
| <a id="s-c4e479ca84"></a>`owner` | `"a-riverhog-ftp-spool-client"` |

## Governing policies

- <a id="pa-5e507c05d7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool-client:A_RIVERHOG_FTP_SPOOL_BASE_URL](../../../evidence/sources/authorities.md#src-b655567212) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::RiverhogFtpSpoolClient.\_\_init\_\_](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool-client` | [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py) | `os.getenv('A_RIVERHOG_FTP_SPOOL_BASE_URL')` |

### Machine authority

- `/external_contract/configuration_environment/59`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f3816cb1015ba6ee480fc0eaa7a79cc165163f571b525906e67a265cf0258e2 -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_BASE_URL",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FTP_SPOOL_BASE_URL",
  "owner": "a-riverhog-ftp-spool-client"
}
```

</details>
