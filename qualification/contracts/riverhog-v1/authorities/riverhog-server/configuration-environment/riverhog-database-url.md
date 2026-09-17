# RIVERHOG_DATABASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-database-url:f9f18a5514 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-79f8ec8b66"></a>

| Field | Value |
|---|---|
| <a id="s-3b66af5af9"></a>`consumers` | `["riverhog-server"]` |
| <a id="s-f033f04804"></a>`default_expressions` | `["''"]` |
| <a id="s-523002be62"></a>`id` | `"riverhog-server:environment:RIVERHOG_DATABASE_URL"` |
| <a id="s-08d11a5f6d"></a>`input_shape` | `"environment-string"` |
| <a id="s-af9e9db7e5"></a>`name` | `"RIVERHOG_DATABASE_URL"` |
| <a id="s-228a0a0fe8"></a>`owner` | `"riverhog-server"` |

## Governing policies

- <a id="pa-d24de4ccc7"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_DATABASE_URL](../../../evidence/sources/authorities.md#src-c84b874748) — [riverhog/src/riverhog\_core/runtime\_config.py::load\_runtime\_config](../../../../../../riverhog/src/riverhog_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | [riverhog/src/riverhog\_core/runtime\_config.py](../../../../../../riverhog/src/riverhog_core/runtime_config.py) | `os.getenv('RIVERHOG_DATABASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/57`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59a96c089bc753ed30c2909868d5f4dbf440296dde0417ba1f192e8d68541c31 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "riverhog-server:environment:RIVERHOG_DATABASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_DATABASE_URL",
  "owner": "riverhog-server"
}
```

</details>
