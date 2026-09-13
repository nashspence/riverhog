# RIVERHOG_FTP_ADAPTER_CONFIG

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-ftp-adapter-config:b99544648f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [settings](index.md#f-fbe482a5e7) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0ef9db2534"></a>
| Field | Shape |
|---|---|
| <a id="s-3d50fc447d"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-82213007c3"></a>`default_expressions` | ["''"] |
| <a id="s-35f99da18f"></a>`id` | "riverhog-ftp-adapter:environment:RIVERHOG_FTP_ADAPTER_CONFIG" |
| <a id="s-f9a70e5c6c"></a>`input_shape` | "environment-string" |
| <a id="s-8eed53d4d3"></a>`name` | "RIVERHOG_FTP_ADAPTER_CONFIG" |
| <a id="s-282ecf4905"></a>`owner` | "riverhog-ftp-adapter" |

## Governing policies

- <a id="pa-94ac24c25a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_FTP_ADAPTER_CONFIG](../../../evidence/sources.md#src-67c239e18e) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_FTP_ADAPTER_CONFIG', '')` |
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_FTP_ADAPTER_CONFIG', '')` |

### Machine authority

- `/external_contract/configuration_environment/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a80ed8da224c046303f12bd28967905a471c8b726197403043a8af6cc64213e -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_FTP_ADAPTER_CONFIG",
  "input_shape": "environment-string",
  "name": "RIVERHOG_FTP_ADAPTER_CONFIG",
  "owner": "riverhog-ftp-adapter"
}
```
