# A_RIVERHOG_FTP_SPOOL_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool:a-riverhog-ftp-spool-config:0fd5c7f595 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-79f8ec8b66"></a>

| Field | Value |
|---|---|
| <a id="s-3b66af5af9"></a>`consumers` | `["a-riverhog-ftp-spool"]` |
| <a id="s-f033f04804"></a>`default_expressions` | `["''"]` |
| <a id="s-523002be62"></a>`id` | `"a-riverhog-ftp-spool:environment:A_RIVERHOG_FTP_SPOOL_CONFIG"` |
| <a id="s-08d11a5f6d"></a>`input_shape` | `"environment-string"` |
| <a id="s-af9e9db7e5"></a>`name` | `"A_RIVERHOG_FTP_SPOOL_CONFIG"` |
| <a id="s-228a0a0fe8"></a>`owner` | `"a-riverhog-ftp-spool"` |

## Governing policies

- <a id="pa-d71b25d961"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool:A_RIVERHOG_FTP_SPOOL_CONFIG](../../../evidence/sources/authorities.md#src-c7e9e6cdab) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::load\_config](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py); [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::load\_source\_config](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool` | [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py) | `os.environ.get('A_RIVERHOG_FTP_SPOOL_CONFIG', '')` |
| parser | `a-riverhog-ftp-spool` | [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py) | `os.environ.get('A_RIVERHOG_FTP_SPOOL_CONFIG', '')` |

### Machine authority

- `/external_contract/configuration_environment/57`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 949dbdce5e3307b21e9063f89c9509d9eb16b96cbe2e1361b9a2294c5a24cd8a -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-ftp-spool:environment:A_RIVERHOG_FTP_SPOOL_CONFIG",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_FTP_SPOOL_CONFIG",
  "owner": "a-riverhog-ftp-spool"
}
```

</details>
