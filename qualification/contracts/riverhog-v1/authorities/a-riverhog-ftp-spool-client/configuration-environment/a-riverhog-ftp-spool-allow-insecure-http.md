# A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-allow-insecure-http:25be65ec32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d8ed99c25"></a>

| Field | Value |
|---|---|
| <a id="s-455d12eaca"></a>`consumers` | `["a-riverhog-ftp-spool-client"]` |
| <a id="s-1072ddffa8"></a>`default_expressions` | `["unset"]` |
| <a id="s-70cf382471"></a>`id` | `"a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP"` |
| <a id="s-1b2f5581d2"></a>`input_shape` | `"environment-string"` |
| <a id="s-1b616d0548"></a>`name` | `"A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP"` |
| <a id="s-8d577f9560"></a>`owner` | `"a-riverhog-ftp-spool-client"` |

## Governing policies

- <a id="pa-b8675737ed"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool-client:A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP](../../../evidence/sources/authorities.md#src-56e5e5eb30) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::\_bool\_env](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool-client` | [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/114`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbc127d5fbaf56bedc87418d0df6c972f2d0f4554d863cd45953ce71ec25153a -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FTP_SPOOL_ALLOW_INSECURE_HTTP",
  "owner": "a-riverhog-ftp-spool-client"
}
```

</details>
