# A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool-client:a-riverhog-ftp-spool-http-timeout-seconds:bff34ce37a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-client](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-03d76831f6"></a>

| Field | Value |
|---|---|
| <a id="s-9d176682fc"></a>`consumers` | `["a-riverhog-ftp-spool-client"]` |
| <a id="s-6af6fd9c96"></a>`default_expressions` | `["unset"]` |
| <a id="s-70fa9907ac"></a>`id` | `"a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-79dd557f69"></a>`input_shape` | `"environment-string"` |
| <a id="s-709f3f0d7c"></a>`name` | `"A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS"` |
| <a id="s-3cae61146e"></a>`owner` | `"a-riverhog-ftp-spool-client"` |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

Shared facts for every subject below: configuration="A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS"; consumers=["a-riverhog-ftp-spool-client"]; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS](#s-03d76831f6) | `value · configured-value · operational_policy` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-86e08d8a83"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-200cd6cc5d"></a>[extent-rule/configured-capacity/v1](../../extent-contract/extent/extent-rule-configured-capacity.md#p-3ebc61fc99)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool-client:A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS](../../../evidence/sources/authorities.md#src-3d1d2bfa7c) — [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py::\_timeout\_env](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool-client` | [some-implementations/riverhog/ingress/ftp-api-client/src/a\_riverhog\_ftp\_spool\_client/client.py](../../../../../../some-implementations/riverhog/ingress/ftp-api-client/src/a_riverhog_ftp_spool_client/client.py) | `os.getenv(name)` |

### Machine authority

- `/external_contract/configuration_environment/117`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60bc3e039574df35ede100b7771798b43e941eac9b37d0d1d58c2db55bc17d21 -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool-client"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-riverhog-ftp-spool-client:environment:A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FTP_SPOOL_HTTP_TIMEOUT_SECONDS",
  "owner": "a-riverhog-ftp-spool-client"
}
```

</details>
