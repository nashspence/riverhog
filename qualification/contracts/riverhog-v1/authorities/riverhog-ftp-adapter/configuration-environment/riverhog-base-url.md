# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-base-url:66c9b4fe8a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1b7215e251"></a>

| Field | Value |
|---|---|
| <a id="s-84c48e5200"></a>`consumers` | `["riverhog-ftp-adapter"]` |
| <a id="s-4f7fd18cab"></a>`default_expressions` | `["''"]` |
| <a id="s-11b04fc0e6"></a>`id` | `"riverhog-ftp-adapter:environment:RIVERHOG_BASE_URL"` |
| <a id="s-503bc11db4"></a>`input_shape` | `"environment-string"` |
| <a id="s-399d989cc4"></a>`name` | `"RIVERHOG_BASE_URL"` |
| <a id="s-03a4e23000"></a>`owner` | `"riverhog-ftp-adapter"` |

## Governing policies

- <a id="pa-f944fdb23d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_BASE_URL](../../../evidence/sources/authorities.md#src-b93808cfe5) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/config.py::load\_config](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter` | [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/config.py](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py) | `os.environ.get('RIVERHOG_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/25`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f783c60e0caf78160b1417a36052c6d664d71e176169517536965319aa5da4cf -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BASE_URL",
  "owner": "riverhog-ftp-adapter"
}
```

</details>
