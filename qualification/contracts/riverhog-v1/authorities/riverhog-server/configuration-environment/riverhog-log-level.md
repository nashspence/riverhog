# RIVERHOG_LOG_LEVEL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-log-level:469968a981 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-baf83c7110"></a>

| Field | Value |
|---|---|
| <a id="s-a2fa2bd77d"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-3fc7fae88b"></a>`default_expressions` | `["DEFAULT_LOG_LEVEL"]` |
| <a id="s-a54273fb17"></a>`id` | `"riverhog-server:environment:RIVERHOG_LOG_LEVEL"` |
| <a id="s-ea0e7b5fd3"></a>`input_shape` | `"environment-string"` |
| <a id="s-facd7c0dcf"></a>`name` | `"RIVERHOG_LOG_LEVEL"` |
| <a id="s-a72677a8ab"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-61c46879f3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_LOG_LEVEL](../../../evidence/sources/authorities.md#src-cb4528636e) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_LOG_LEVEL', DEFAULT_LOG_LEVEL)` |

### Machine authority

- `/external_contract/configuration_environment/196`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1486e6dc0b3d045996c4f025f3ad92c19e74c4501e335d855e314d4196eaf6ad -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "DEFAULT_LOG_LEVEL"
  ],
  "id": "riverhog-server:environment:RIVERHOG_LOG_LEVEL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_LOG_LEVEL",
  "owner": "riverhog-server"
}
```

</details>
